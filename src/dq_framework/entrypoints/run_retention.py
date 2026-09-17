"""
Entrypoint CDE per il job di retention (pulizia) delle tabelle di Data Quality.

Applica la retention dichiarata nella documentazione del framework alle due
tabelle Iceberg prodotte dalla pipeline di quality:

    <dl_layer>_dqf_<data_product>_results          -> 730 giorni (2 anni)
    <dl_layer>_dqf_<data_product>_failed_records   ->  90 giorni

Entrambe sono partizionate per `execution_date`, quindi la cancellazione usa un
predicato allineato alla partizione (`execution_date < DATE '...'`): Iceberg
elimina interi data file dal manifest senza riscrivere nulla, invece di
produrre delete file (le tabelle sono dichiarate merge-on-read).

La DELETE da sola non libera spazio su storage: i vecchi snapshot continuano a
referenziare i file rimossi. Per questo, di default, dopo ogni DELETE il job
chiama `system.expire_snapshots`. Non viene invece chiamata
`remove_orphan_files`, piu' invasiva e da pianificare separatamente.

Schedulazione prevista: giornaliera, un run per database.

Invocazione tipica su CDE (con un application file dedicato, vedi launcher.py):
    spark-submit launcher_retention.py --database pagopa_dev

Invocazione locale / manuale:
    ENV=test python -m dq_framework.entrypoints.run_retention --database pagopa_dev
    ENV=test python -m dq_framework.entrypoints.run_retention --dry-run

Il database di default e' quello dell'ambiente corrente
(`AppConfig.results_database`: `pagopa_dev` in dev/test, `pagopa_qa` in prod),
sovrascrivibile con `--database`.

Il perimetro e' la lista esplicita `DQF_TABLES` in testa a questo modulo: il
job non interroga il metastore per scoprire le tabelle, quindi cosa viene
pulito e' leggibile nel codice e non dipende dallo stato del database. Una
tabella nuova va aggiunta a mano alla lista. I flag `--domain`/`--dl-layer`
servono solo a restringere quella lista per un run puntuale (accettano piu'
valori separati da virgola).

Guard-rail: se l'ambiente ha `results_write_enabled=False` (es. `dev`) il job
degrada automaticamente a dry-run, coerentemente con la pipeline di quality che
in quell'ambiente non scrive su DB.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession

from dq_framework.common.config import AppConfig, load_config
from dq_framework.common.logging import setup_logging

logger = logging.getLogger(__name__)

# Retention di default, dalla documentazione delle tabelle DQF.
DEFAULT_RESULTS_RETENTION_DAYS = 730          # 2 anni
DEFAULT_FAILED_RECORDS_RETENTION_DAYS = 90    # 90 giorni

# Giorni di storico snapshot da mantenere quando si chiama expire_snapshots:
# lascia una finestra di time travel / rollback dopo la cancellazione.
DEFAULT_SNAPSHOT_RETENTION_DAYS = 7

_SQL_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Naming convention DQF: <dl_layer>_dqf_<data_product>_<results|failed_records>.
# Il data product puo' contenere underscore (es. gold_dqf_gpd_gec_reconciliation_results),
# quindi il gruppo e' greedy e viene disambiguato dal suffisso ancorato a fine stringa.
_DQF_TABLE_RE = re.compile(
    r"^(?P<dl_layer>[a-z][a-z0-9]*)_dqf_(?P<domain>[a-z0-9_]+)_(?P<suffix>results|failed_records)$"
)

# Perimetro del job: elenco esplicito delle tabelle da pulire.
#
# Volutamente NON si interroga il metastore (SHOW TABLES + filtro sul pattern):
# una lista dichiarata rende il perimetro leggibile qui, uguale a ogni run e
# rivedibile in code review, senza dipendere da cosa c'e' nel database al
# momento dell'esecuzione. Il prezzo e' che una tabella nuova va aggiunta a
# mano: il job segnala a WARNING quelle dichiarate ma assenti, non quelle
# presenti e non dichiarate.
#
# La retention non e' ripetuta per tabella: si deriva dal suffisso del nome
# (results / failed_records) e resta governata dai flag CLI.
DQF_TABLES: tuple[str, ...] = (
    "silver_dqf_fdr_failed_records",
    "silver_dqf_fdr_results",
    "silver_dqf_gec_failed_records",
    "silver_dqf_gec_results",
    "silver_dqf_gpd_failed_records",
    "silver_dqf_gpd_results",
    "silver_dqf_wallet_failed_records",
    "silver_dqf_wallet_results",
)


@dataclass(frozen=True)
class RetentionTarget:
    """Una tabella da pulire, con la finestra di retention gia' risolta."""

    table: str          # nome semplice (es. silver_dqf_gpd_results)
    fqn: str            # nome completo (es. pagopa_dev.silver_dqf_gpd_results)
    suffix: str         # "results" | "failed_records"
    dl_layer: str
    domain: str
    retention_days: int
    cutoff: date        # si cancella tutto cio' che ha execution_date < cutoff


# =========================================================================
# Parser argparse
# =========================================================================
def _parse_sql_identifier(value: str) -> str:
    """Valida un identificatore SQL semplice (stessa logica di run_quality).

    Serve perche' database, domain e dl_layer finiscono interpolati nella FQN:
    un valore con un punto produrrebbe un FQN a tre parti, che Spark interpreta
    come catalog.database.table e che quindi cancellerebbe righe altrove.
    """
    normalized = value.strip().lower()
    if not _SQL_IDENTIFIER_RE.match(normalized):
        raise argparse.ArgumentTypeError(
            f"Valore non valido: {value!r}. Ammessi solo identificatori SQL semplici "
            f"(minuscole, cifre e underscore, iniziale alfabetica), es. 'pagopa_dev', 'gpd', 'silver'."
        )
    return normalized


def _parse_identifier_list(value: str) -> list[str]:
    """Parser per --domain / --dl-layer: lista separata da virgola."""
    return [_parse_sql_identifier(item) for item in value.split(",") if item.strip()]


def _parse_retention_days(value: str) -> int:
    """Parser per i giorni di retention: intero >= 0 (0 = nessuno storico conservato)."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Valore non numerico: {value!r}.") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError(f"La retention non puo' essere negativa: {value!r}.")
    return parsed


def _parse_iso_date(value: str) -> date:
    """Parser per --reference-date. Accetta 'YYYY-MM-DD' o un timestamp ISO 8601."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--reference-date non e' una data ISO valida: {value!r}. Esempio: '2026-08-27'."
        ) from exc


def _parse_args(default_database: str, argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retention delle tabelle DQF (results / failed_records)")

    parser.add_argument(
        "--database",
        type=_parse_sql_identifier,
        default=default_database,
        help=(
            "Database del Data Lake su cui applicare la retention. "
            f"Default: valore configurato per l'ambiente corrente ({default_database})."
        ),
    )
    parser.add_argument(
        "--domain",
        type=_parse_identifier_list,
        default=None,
        help=(
            "Filtro sui data product da pulire, separati da virgola (es. 'gpd,gec'). "
            "Se omesso vengono pulite tutte le tabelle di DQF_TABLES."
        ),
    )
    parser.add_argument(
        "--dl-layer",
        type=_parse_identifier_list,
        default=None,
        help=(
            "Filtro sui layer del Data Lake, separati da virgola (es. 'silver,gold'). "
            "Se omesso vengono puliti tutti i layer presenti in DQF_TABLES."
        ),
    )
    parser.add_argument(
        "--results-retention-days",
        type=_parse_retention_days,
        default=DEFAULT_RESULTS_RETENTION_DAYS,
        help=(
            "Giorni di storico da mantenere nelle tabelle *_results. "
            f"Default: {DEFAULT_RESULTS_RETENTION_DAYS} (2 anni)."
        ),
    )
    parser.add_argument(
        "--failed-records-retention-days",
        type=_parse_retention_days,
        default=DEFAULT_FAILED_RECORDS_RETENTION_DAYS,
        help=(
            "Giorni di storico da mantenere nelle tabelle *_failed_records. "
            f"Default: {DEFAULT_FAILED_RECORDS_RETENTION_DAYS}."
        ),
    )
    parser.add_argument(
        "--reference-date",
        type=_parse_iso_date,
        default=None,
        help=(
            "Data di riferimento da cui calcolare il cutoff (cutoff = reference_date - retention). "
            "Default: data odierna UTC. Utile per recuperi manuali e per allineare il cutoff "
            "alla logical date del DAG (ds)."
        ),
    )
    parser.add_argument(
        "--catalog",
        default="spark_catalog",
        help=(
            "Catalogo Iceberg usato per invocare le stored procedure di manutenzione "
            "(es. spark_catalog.system.expire_snapshots). Default: spark_catalog."
        ),
    )
    parser.add_argument(
        "--snapshot-retention-days",
        type=_parse_retention_days,
        default=DEFAULT_SNAPSHOT_RETENTION_DAYS,
        help=(
            "Giorni di snapshot Iceberg da mantenere in expire_snapshots, cioe' la finestra "
            f"di time travel/rollback residua. Default: {DEFAULT_SNAPSHOT_RETENTION_DAYS}."
        ),
    )
    parser.add_argument(
        "--snapshot-retain-last",
        type=_parse_retention_days,
        default=1,
        help="Numero minimo di snapshot da conservare comunque (retain_last). Default: 1.",
    )
    parser.add_argument(
        "--skip-expire-snapshots",
        action="store_true",
        help=(
            "Esegue solo la DELETE, senza expire_snapshots: le righe non sono piu' "
            "interrogabili ma i file restano su storage fino alla prossima expire."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Conta le righe fuori retention e logga il piano, senza cancellare nulla.",
    )
    parser.add_argument(
        "--dag-id",
        default=None,
        help="Identificativo del DAG Airflow, solo a scopo di log e tracciamento.",
    )
    return parser.parse_args(argv)


# =========================================================================
# Spark
# =========================================================================
def init_spark(app_name: str = "dqf_retention") -> SparkSession:
    logger.info(f"Inizializzazione SparkSession (appName={app_name})...")
    return (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )


# =========================================================================
# Risoluzione dei target
# =========================================================================
def parse_declared_tables(tables: tuple[str, ...] = DQF_TABLES) -> list[tuple[str, re.Match]]:
    """Valida i nomi dichiarati in DQF_TABLES contro la naming convention.

    Fail-fast all'avvio, prima di aprire la SparkSession: un nome che non
    rispetta la convenzione non ha un suffisso da cui derivare la retention,
    quindi verrebbe silenziosamente ignorato. Meglio un errore immediato che
    una tabella mai pulita senza che nessuno se ne accorga.
    """
    parsed: list[tuple[str, re.Match]] = []
    invalid: list[str] = []
    for name in tables:
        match = _DQF_TABLE_RE.match(name)
        if match:
            parsed.append((name, match))
        else:
            invalid.append(name)

    if invalid:
        raise ValueError(
            f"Nomi non conformi alla naming convention DQF in DQF_TABLES: {invalid}. "
            f"Attesi nomi del tipo '<dl_layer>_dqf_<data_product>_<results|failed_records>'."
        )
    if not parsed:
        raise ValueError("DQF_TABLES e' vuota: nessuna tabella da pulire.")
    return sorted(parsed, key=lambda item: item[0])


def resolve_targets(
    spark: SparkSession,
    database: str,
    retention_by_suffix: dict[str, int],
    reference_date: date,
    domains: list[str] | None = None,
    dl_layers: list[str] | None = None,
) -> tuple[list[RetentionTarget], list[str]]:
    """Costruisce i target dalla lista dichiarata, applicando gli eventuali filtri.

    Ritorna (target da pulire, tabelle dichiarate ma assenti dal database).
    L'assenza non e' fatale per tabella - un ambiente puo' non avere ancora
    tutti i domini - ma viene segnalata a WARNING e riportata nel riepilogo,
    perche' e' anche il sintomo di una tabella rinominata e quindi mai piu'
    pulita.
    """
    targets: list[RetentionTarget] = []
    missing: list[str] = []

    for name, match in parse_declared_tables():
        dl_layer = match.group("dl_layer")
        domain = match.group("domain")
        suffix = match.group("suffix")

        if domains and domain not in domains:
            logger.debug(f"Tabella {name} scartata: data product '{domain}' non nei filtri {domains}.")
            continue
        if dl_layers and dl_layer not in dl_layers:
            logger.debug(f"Tabella {name} scartata: layer '{dl_layer}' non nei filtri {dl_layers}.")
            continue

        fqn = f"{database}.{name}"
        if not _table_exists(spark, fqn):
            logger.warning(f"Tabella dichiarata ma assente in {database}: {name} - saltata.")
            missing.append(name)
            continue

        retention_days = retention_by_suffix[suffix]
        targets.append(RetentionTarget(
            table          = name,
            fqn            = fqn,
            suffix         = suffix,
            dl_layer       = dl_layer,
            domain         = domain,
            retention_days = retention_days,
            cutoff         = reference_date - timedelta(days=retention_days),
        ))
    return targets, missing


def _table_exists(spark: SparkSession, fqn: str) -> bool:
    """Verifica l'esistenza della tabella. Qualsiasi errore vale come assente."""
    try:
        return bool(spark.catalog.tableExists(fqn))
    except Exception as exc:  # noqa: BLE001 - database inesistente, permessi, catalogo giu'
        logger.warning(f"Verifica di esistenza fallita per {fqn}: {exc}")
        return False


# =========================================================================
# Operazioni sulla singola tabella
# =========================================================================
def count_expiring_rows(spark: SparkSession, target: RetentionTarget) -> int | None:
    """Conta le righe fuori retention. Ritorna None se il conteggio fallisce.

    Il conteggio serve solo all'osservabilita': Iceberg non restituisce il
    numero di righe cancellate dalla DELETE, quindi senza questa query il log
    non direbbe quanto e' stato purgato. Il predicato e' allineato alla
    partizione, cosi' la scansione tocca solo le partizioni interessate.
    """
    try:
        row = spark.sql(
            f"SELECT COUNT(*) AS n FROM {target.fqn} "
            f"WHERE execution_date < DATE '{target.cutoff.isoformat()}'"
        ).collect()[0]
        return int(row["n"])
    except Exception as exc:  # noqa: BLE001 - il conteggio non deve far fallire la pulizia
        logger.warning(f"Conteggio righe fuori retention non disponibile per {target.fqn}: {exc}")
        return None


def delete_expired_rows(spark: SparkSession, target: RetentionTarget, label: str = "") -> None:
    """Cancella le righe con execution_date < cutoff.

    `label` e' solo il prefisso di log (es. "[3/25 silver_dqf_gpd_results]"),
    per tenere ogni riga della traccia agganciata alla sua tabella.
    """
    stmt = f"DELETE FROM {target.fqn} WHERE execution_date < DATE '{target.cutoff.isoformat()}'"
    logger.info(f"{label} esecuzione: {stmt}".lstrip())
    spark.sql(stmt)


# Etichette leggibili per le colonne del report di expire_snapshots. Il set
# esatto di colonne dipende dalla versione di Iceberg sul cluster, quindi i nomi
# NON sono cablati: si legge il Row cosi' com'e' e le chiavi non previste
# finiscono nel log col loro nome grezzo.
_EXPIRE_COUNT_LABELS = {
    "deleted_data_files_count":             "data file",
    "deleted_position_delete_files_count":  "position delete file",
    "deleted_equality_delete_files_count":  "equality delete file",
    "deleted_manifest_files_count":         "manifest",
    "deleted_manifest_lists_count":         "manifest list",
    "deleted_statistics_files_count":       "file di statistiche",
}


def _extract_expire_counts(rows: list) -> dict[str, int]:
    """Normalizza il report di expire_snapshots in un dict {colonna: conteggio}."""
    counts: dict[str, int] = {}
    if not rows:
        return counts
    try:
        raw = rows[0].asDict()
    except AttributeError:
        logger.debug(f"Report expire_snapshots in formato inatteso: {rows[0]!r}")
        return counts
    for key, value in raw.items():
        if value is None:
            continue
        try:
            counts[key] = int(value)
        except (TypeError, ValueError):
            logger.debug(f"Colonna '{key}' del report expire_snapshots non numerica: {value!r}")
    return counts


def _format_expire_counts(counts: dict[str, int]) -> str:
    """Rende il report leggibile: '12 data file, 3 manifest, 1 manifest list rimossi'.

    I conteggi a zero sono omessi per non allungare la riga; se sono tutti zero
    si esplicita il motivo tipico, cioe' che nessuno snapshot ha ancora
    superato la soglia `older_than` (lo spazio si libera con qualche giorno di
    ritardo rispetto alla DELETE, by design).
    """
    if not counts:
        return "nessun conteggio restituito dalla procedura"
    non_zero = {key: value for key, value in counts.items() if value}
    if not non_zero:
        return "nessun file rimosso (nessuno snapshot oltre la soglia older_than)"
    parts = [f"{value} {_EXPIRE_COUNT_LABELS.get(key, key)}" for key, value in non_zero.items()]
    return ", ".join(parts) + " rimossi"


def expire_snapshots(
    spark: SparkSession,
    target: RetentionTarget,
    catalog: str,
    database: str,
    older_than: datetime,
    retain_last: int,
    label: str = "",
) -> dict[str, int]:
    """Chiama system.expire_snapshots per liberare i file dereferenziati dalla DELETE.

    La procedura agisce su UNA tabella (parametro `table`) ed e' guidata dai
    metadata: cancella solo i file referenziati dagli snapshot scaduti e da
    nessuno degli snapshot sopravvissuti. Non elenca directory, quindi non puo'
    toccare file di altre tabelle (a differenza di remove_orphan_files).

    Ritorna il report dei file effettivamente rimossi, per il log.
    """
    stmt = (
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{database}.{target.table}', "
        f"older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}', "
        f"retain_last => {retain_last})"
    )
    logger.info(f"{label} esecuzione: {stmt}".lstrip())
    return _extract_expire_counts(spark.sql(stmt).collect())


# =========================================================================
# Orchestrazione
# =========================================================================
def run_retention(args: argparse.Namespace, config: AppConfig) -> int:
    """Applica la retention a tutte le tabelle target. Ritorna il numero di errori."""
    reference_date = args.reference_date or datetime.now(timezone.utc).date()
    retention_by_suffix = {
        "results": args.results_retention_days,
        "failed_records": args.failed_records_retention_days,
    }

    # In dev la pipeline di quality non scrive su DB: coerentemente, qui non si
    # cancella nulla. Il job resta eseguibile in locale come sola simulazione.
    dry_run = args.dry_run
    if not config.results_write_enabled and not dry_run:
        logger.warning(
            f"results_write_enabled=False (env={config.env}): il job degrada a dry-run, "
            f"nessuna riga verra' cancellata."
        )
        dry_run = True

    logger.info("+" * 80)
    logger.info(
        f"Avvio retention DQF su database={args.database} (env={config.env}, dag_id={args.dag_id}) "
        f"reference_date={reference_date.isoformat()} dry_run={dry_run}"
    )
    logger.info(
        f"Retention: results={args.results_retention_days}g "
        f"failed_records={args.failed_records_retention_days}g"
    )

    # Validazione della lista dichiarata prima di aprire la SparkSession: un
    # nome non conforme e' un errore di configurazione, inutile pagare
    # l'avvio del cluster per scoprirlo.
    declared = parse_declared_tables()
    logger.info(f"Perimetro dichiarato (DQF_TABLES): {[name for name, _ in declared]}")

    spark = init_spark(app_name=f"dqf_retention_{args.database}")
    errors = 0
    try:
        targets, missing = resolve_targets(
            spark               = spark,
            database            = args.database,
            retention_by_suffix = retention_by_suffix,
            reference_date      = reference_date,
            domains             = args.domain,
            dl_layers           = args.dl_layer,
        )

        if not targets:
            if missing:
                # Nessuna delle tabelle dichiarate esiste: non e' drift di un
                # ambiente parziale, e' database sbagliato o permessi mancanti.
                # Va trattato come errore, altrimenti il job esce verde senza
                # aver pulito niente.
                logger.error(
                    f"Nessuna delle {len(missing)} tabelle dichiarate esiste in {args.database}: "
                    f"{missing}. Verificare --database, ENV e i permessi."
                )
                return 1
            logger.warning(
                f"Nessuna tabella da pulire in {args.database} "
                f"(filtri: domain={args.domain}, dl_layer={args.dl_layer})."
            )
            return 0

        total = len(targets)
        snapshot_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=args.snapshot_retention_days
        )

        # Piano di esecuzione: prima si stampa cosa verra' fatto e con quali
        # cutoff, poi si esegue. Serve a poter verificare le date a colpo
        # d'occhio nel log del run, prima di andare a leggere le singole DELETE.
        logger.info(
            f"Tabelle dichiarate: {len(DQF_TABLES)}, da pulire in {args.database}: {total}"
            + (f", assenti: {len(missing)} {missing}" if missing else "")
        )
        for idx, target in enumerate(targets, start=1):
            logger.info(
                f"  {idx:>3}/{total}  {target.table:<50} retention={target.retention_days:>4}g  "
                f"cutoff=execution_date < {target.cutoff.isoformat()}"
            )
        if not args.skip_expire_snapshots and not dry_run:
            logger.info(
                f"Dopo ogni DELETE: expire_snapshots con older_than="
                f"{snapshot_cutoff.strftime('%Y-%m-%d %H:%M:%S')} (retain_last={args.snapshot_retain_last})."
            )

        # Totali di run, per il riepilogo finale.
        total_expiring_rows = 0
        total_expire_counts: dict[str, int] = {}
        deleted_tables = 0
        expired_tables = 0

        for idx, target in enumerate(targets, start=1):
            label = f"[{idx}/{total} {target.table}]"
            logger.info("-" * 80)
            logger.info(
                f"{label} inizio: retention={target.retention_days}g, "
                f"cancellazione di execution_date < {target.cutoff.isoformat()}"
            )

            expiring = count_expiring_rows(spark, target)
            if expiring is not None:
                total_expiring_rows += expiring
                logger.info(f"{label} righe fuori retention: {expiring}")

            if dry_run:
                logger.info(f"{label} dry-run: DELETE e expire_snapshots non eseguite.")
                continue

            if expiring == 0:
                logger.info(f"{label} nulla fuori retention, DELETE saltata.")
            else:
                try:
                    delete_expired_rows(spark, target, label=label)
                    deleted_tables += 1
                    logger.info(f"{label} DELETE completata.")
                except Exception as exc:  # noqa: BLE001 - una tabella rotta non ferma le altre
                    errors += 1
                    logger.error(f"{label} DELETE fallita: {exc}", exc_info=True)
                    # Senza DELETE riuscita l'expire non ha senso: si passa alla tabella dopo.
                    continue

            if args.skip_expire_snapshots:
                logger.info(f"{label} expire_snapshots saltata (--skip-expire-snapshots).")
                continue

            try:
                counts = expire_snapshots(
                    spark       = spark,
                    target      = target,
                    catalog     = args.catalog,
                    database    = args.database,
                    older_than  = snapshot_cutoff,
                    retain_last = args.snapshot_retain_last,
                    label       = label,
                )
                expired_tables += 1
                for key, value in counts.items():
                    total_expire_counts[key] = total_expire_counts.get(key, 0) + value
                logger.info(f"{label} expire_snapshots completata: {_format_expire_counts(counts)}.")
            except Exception as exc:  # noqa: BLE001 - la retention logica e' comunque applicata
                # Non incrementa `errors`: le righe sono state cancellate e non
                # sono piu' interrogabili, resta solo spazio non ancora liberato.
                logger.warning(
                    f"{label} expire_snapshots fallita, i file restano su storage "
                    f"fino al prossimo run: {exc}"
                )

        logger.info("-" * 80)
        logger.info(
            f"Riepilogo: {total} tabelle esaminate, {total_expiring_rows} righe fuori retention, "
            f"{deleted_tables} DELETE eseguite, {expired_tables} expire_snapshots eseguite, "
            f"{len(missing)} tabelle dichiarate assenti (dry_run={dry_run}, errori={errors})."
        )
        if expired_tables:
            logger.info(f"Storage liberato in totale: {_format_expire_counts(total_expire_counts)}.")
        return errors
    finally:
        logger.info("Chiusura sessione Spark.")
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    config = load_config()
    args = _parse_args(default_database=config.results_database, argv=argv)

    errors = run_retention(args, config)
    if errors:
        # Exit code != 0 -> spark-submit fallisce e CDE marca il run come FAILED.
        logger.error(f"Job di retention concluso con {errors} tabelle in errore.")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
