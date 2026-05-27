from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession

from dq_framework.common.config import AppConfig
from .contract_parser import parse_contract_file
from .result_writer import RESULTS_SCHEMA, new_run_id, process_scan_results
from .soda_executor import run_dataframe_soda_scan

logger = logging.getLogger(__name__)

def _lookup_check_watermark(
    spark: SparkSession,
    config: AppConfig,
    dataset: str,
    check_name: str,
) -> Optional[datetime]:
    """Restituisce il massimo `watermark_to` registrato per il check specificato.

    Filtra sulla tabella results con `outcome = 'pass'` e
    `watermark_to IS NOT NULL`: in questo modo eventuali run precedenti in
    `warn` o `fail` non avanzano il watermark di quel check. Se la tabella non
    esiste, e' vuota o il lookup esplode per qualsiasi ragione, restituisce
    `None` cosi' che il chiamante possa applicare la logica di bootstrap.
    """
    fqn = f"{config.results_database}.{config.results_table}"
    try:
        row = spark.sql(
            f"""
            SELECT MAX(watermark_to) AS wm
            FROM {fqn}
            WHERE dataset = '{dataset}'
              AND check_name = '{check_name}'
              AND outcome = 'pass'
              AND watermark_to IS NOT NULL
            """
        ).collect()
        return row[0]["wm"] if row and row[0]["wm"] else None
    except Exception as e:
        logger.warning(
            f"Lookup watermark fallito per check='{check_name}' su {fqn}: {e}. "
            f"Si procedera' in bootstrap."
        )
        return None


def _build_incremental_conditions(
    watermark_column: str,
    wm_from: datetime,
    wm_to: datetime,
) -> str:
    """Genera la clausola SQL da sostituire al placeholder.

    Forma esatta (estremo sinistro escluso, destro incluso):
        <col> > TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ffffff'
          AND <col> <= TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ffffff'

    I literal TIMESTAMP con microsecondi sono pushdown-friendly su Iceberg via
    Catalyst, e su tabelle partizionate per `DAY(<col>)` permettono partition
    pruning aggressivo.
    """
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return (
        f"{watermark_column} > TIMESTAMP '{wm_from.strftime(fmt)}' "
        f"AND {watermark_column} <= TIMESTAMP '{wm_to.strftime(fmt)}'"
    )


# In SodaCL il campo che contiene la SQL custom puo' avere nomi diversi:
#   - statici: "fail query", "failed rows query", "metric query"
#   - dinamici: "<metric_name> query" per i user-defined metric check
#     (es. nel contract: "count_due_date_exceeds_max_due_date query")
# Generalizziamo: una chiave del blocco check e' candidata se finisce con
# " query" (spazio + "query"). Il primo match in ogni check vince.
def _find_query_field(check_body: dict) -> Optional[str]:
    return next(
        (k for k in check_body if isinstance(k, str) and k.endswith(" query")),
        None,
    )


def _resolve_per_check_watermarks(
    spark: SparkSession,
    config: AppConfig,
    contract: dict,
    scan_ts: datetime,
    watermark_column: str,
    cli_override: Optional[datetime],
) -> tuple[str, dict[str, datetime]]:
    """Walk strutturato del SodaCL, sostituzione per-check del placeholder.

    Ritorna:
        - sodacl_yaml: stringa YAML aggiornata con i placeholder sostituiti
          dalla clausola SQL specifica del singolo check.
        - per_check_wm: mapping `check_name -> wm_from` usato da
          `process_scan_results` per popolare le colonne watermark sulle righe
          dei risultati (solo per i check incrementali; quelli massivi non
          compaiono nel dict).

    Logica di risoluzione per ogni check con placeholder presente nella query:
        1. CLI override (se fornito) → applicato a TUTTI i check incrementali.
        2. Lookup su tabella results con `outcome='pass'` e lookback.
        3. Bootstrap a epoch (1970-01-01) con warning log.

    Solleva ValueError se per qualche check `wm_from >= scan_ts`.
    """
    placeholder = config.incremental_placeholder
    spec_dict = yaml.safe_load(contract["sodacl"])
    per_check_wm: dict[str, datetime] = {}

    if not isinstance(spec_dict, dict):
        # SodaCL malformato o vuoto: niente da fare
        return contract["sodacl"], per_check_wm

    for key in spec_dict:
        if not isinstance(key, str) or not key.startswith("checks for "):
            continue

        check_list = spec_dict[key]
        if not isinstance(check_list, list):
            continue

        for check_item in check_list:
            if not isinstance(check_item, dict):
                continue

            for check_type, check_body in check_item.items():
                if not isinstance(check_body, dict):
                    continue

                check_name = check_body.get("name")
                if not check_name:
                    # Senza nome non possiamo fare lookup; skip silente
                    continue

                # Identifica il campo query del check (se presente)
                query_field = _find_query_field(check_body)
                if query_field is None:
                    continue

                query_text = check_body[query_field]
                if not isinstance(query_text, str) or placeholder not in query_text:
                    # Check massivo (o senza placeholder): non lo tocchiamo
                    continue

                # --- Risoluzione wm_from per questo specifico check ---
                if cli_override is not None:
                    wm_from = cli_override
                    source = "cli"
                else:
                    looked_up = _lookup_check_watermark(
                        spark, config, contract["table_name"], check_name
                    )
                    if looked_up is not None:
                        wm_from = looked_up - timedelta(
                            minutes=config.incremental_lookback_minutes
                        )
                        source = "iceberg"
                    else:
                        wm_from = datetime(1970, 1, 1)
                        source = "bootstrap"
                        logger.warning(
                            f"Bootstrap watermark per check '{check_name}': "
                            f"nessuna run 'pass' precedente trovata."
                        )

                if wm_from >= scan_ts:
                    raise ValueError(
                        f"Watermark invalido per check '{check_name}': "
                        f"wm_from={wm_from} >= wm_to={scan_ts}"
                    )

                # --- Sostituzione localizzata del placeholder nella query ---
                conditions_sql = _build_incremental_conditions(
                    watermark_column, wm_from, scan_ts
                )
                check_body[query_field] = query_text.replace(
                    placeholder, conditions_sql
                )
                per_check_wm[check_name] = wm_from

                logger.info(
                    f"Watermark check='{check_name}' "
                    f"column={watermark_column} "
                    f"from={wm_from.isoformat()} to={scan_ts.isoformat()} "
                    f"source={source}"
                )

    sodacl_yaml = yaml.safe_dump(spec_dict, sort_keys=False, allow_unicode=True)
    return sodacl_yaml, per_check_wm


def init_spark() -> SparkSession:
    logger.info("Inizializzazione SparkSession...")
    return (
        SparkSession.builder
        .appName("gpd_quality_pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )


def _log_contract_summary(checks: list[dict], contract_title: str) -> None:
    passed = [c for c in checks if c.get("outcome") == "pass"]
    warned = [c for c in checks if c.get("outcome") == "warn"]
    failed = [c for c in checks if c.get("outcome") == "fail"]
    errors = [c for c in checks if c.get("outcome") == "error"]

    logger.info(
        f"Riepilogo contract [{contract_title}] - "
        f"Totale: {len(checks)} | PASS: {len(passed)} | WARN: {len(warned)} | "
        f"FAIL: {len(failed)} | ERRORS: {len(errors)}"
    )

    for chk in warned + failed + errors:
        diag    = chk.get("diagnostics") or {}
        outcome = chk.get("outcome", "unknown").upper()
        logger.warning(f"[{outcome}] Check: {chk.get('name')} | Valore Rilevato: {diag.get('value')}")


def _log_results_summary(df_results: DataFrame) -> None:
    logger.info("=" * 80)
    logger.info("RISULTATI FINALI PIPELINE DATA QUALITY")
    logger.info("=" * 80)

    for row in df_results.collect():
        outcome_tag = f"[{(row.outcome or '').upper()}]"
        logger.info(f"{outcome_tag} {row.check_name}")
        logger.info(f"    - Dataset  : {row.dataset}")
        logger.info(
            f"    - Misura   : numeric={row.measured_value_numeric} "
            f"string={row.measured_value_string} | "
            f"rows_total={row.row_count_total} rows_failed={row.row_count_failed}"
        )

    logger.info("=" * 80)


_RESULTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
    run_id                 STRING  NOT NULL,
    airflow_run_id         STRING,
    dag_id                 STRING  NOT NULL,
    execution_ts           TIMESTAMP NOT NULL,
    execution_date         DATE    NOT NULL,
    dataset                STRING  NOT NULL,
    check_name             STRING  NOT NULL,
    check_column           STRING,
    check_category         STRING,
    check_dimension        STRING,
    outcome                STRING  NOT NULL,
    measured_value_numeric DOUBLE,
    measured_value_string  STRING,
    threshold_warn         STRING,
    threshold_fail         STRING,
    row_count_total        BIGINT,
    row_count_failed       BIGINT,
    has_failed_records     BOOLEAN NOT NULL,
    watermark_column       STRING,
    watermark_from         TIMESTAMP,
    watermark_to           TIMESTAMP
)
USING iceberg
PARTITIONED BY (execution_date)
{location_clause}
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.parquet.compression-codec'='snappy'
)
""".strip()


def _ensure_results_table(spark: SparkSession, config: AppConfig, fqn: str) -> None:
    location_clause = (
        f"LOCATION '{config.results_table_location}'"
        if config.results_table_location
        else ""
    )
    ddl = _RESULTS_TABLE_DDL.format(fqn=fqn, location_clause=location_clause)
    logger.info(f"Esecuzione CREATE TABLE IF NOT EXISTS su {fqn}")
    spark.sql(ddl)


def _write_results_to_iceberg(spark: SparkSession, df_results: DataFrame, config: AppConfig) -> None:
    if not config.results_write_enabled:
        logger.info(
            f"results_write_enabled=False (env={config.env}): scrittura su DB saltata. "
            f"Risultati solo loggati."
        )
        return

    fqn = f"{config.results_database}.{config.results_table}"
    logger.info(f"Scrittura risultati su tabella Iceberg: {fqn}")
    try:
        _ensure_results_table(spark, config, fqn)
        df_results.writeTo(fqn).append()
        logger.info(f"Scrittura completata: {df_results.count()} record inseriti in {fqn}.")
    except Exception as e:
        logger.error(f"Errore in scrittura su {fqn}: {e}", exc_info=True)
        raise


def run_pipeline(
    contract_path:             str,
    repository:                str,
    ref:                       str,
    config:                    AppConfig,
    dag_id:                    Optional[str]      = None,
    airflow_run_id:            Optional[str]      = None,
    watermark_column_override: Optional[str]      = None,
    watermark_from_override:   Optional[datetime] = None,
) -> None:
    source_desc = f"{repository}@{ref}:{contract_path}" if repository else contract_path
    logger.info(f"Avvio pipeline Data Quality GPD per: {source_desc}")

    contract = parse_contract_file(contract_path, repository, ref, config)
    if not contract:
        logger.error("Contract non valido o non trovato. Pipeline terminata.")
        return

    spark    = init_spark()
    run_id   = new_run_id()
    scan_ts  = datetime.utcnow()   # FREEZE POINT: wm_to globale per tutti i check incrementali
    # dag_id è NOT NULL in tabella: fallback se non fornito né via CLI né via env
    effective_dag_id = dag_id or f"manual:{config.env}"
    all_rows = []

    logger.info("+" * 80)
    logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")
    logger.info(f"run_id={run_id} dag_id={effective_dag_id} airflow_run_id={airflow_run_id}")

    # --- Sezione incrementale ------------------------------------------------
    # Se il SodaCL contiene il placeholder, il framework risolve per-check il
    # watermark_from e sostituisce monoliticamente il placeholder con la
    # clausola SQL "<col> > TIMESTAMP '...' AND <col> <= TIMESTAMP '...'".
    # wm_to e' uguale per tutti i check incrementali (= scan_ts).
    per_check_wm: dict[str, datetime] = {}
    effective_watermark_column: Optional[str] = None

    if config.incremental_placeholder in contract["sodacl"]:
        effective_watermark_column = (
            watermark_column_override
            or config.default_watermark_column
        )
        if not effective_watermark_column:
            raise ValueError(
                f"Contract '{contract['contract_title']}' contiene il placeholder "
                f"{config.incremental_placeholder} ma nessuna colonna watermark e' "
                f"stata fornita (CLI --watermark-column assente e "
                f"AppConfig.default_watermark_column non configurata)."
            )

        logger.info(
            f"Controlli incrementali rilevati. watermark_column={effective_watermark_column} "
            f"scan_ts(wm_to)={scan_ts.isoformat()}"
        )
        new_sodacl, per_check_wm = _resolve_per_check_watermarks(
            spark             = spark,
            config            = config,
            contract          = contract,
            scan_ts           = scan_ts,
            watermark_column  = effective_watermark_column,
            cli_override      = watermark_from_override,
        )
        contract["sodacl"] = new_sodacl
    # --- Fine sezione incrementale -------------------------------------------

    logger.info(f"Esecuzione run_dataframe_soda_scan tramite Soda / PySpark")
    # 1. Esecuzione tramite Soda / PySpark
    soda_checks, total_rows = run_dataframe_soda_scan(spark, contract, config)

    if soda_checks:
        rows = process_scan_results(
            scan_checks       = soda_checks,
            contract_title    = contract["contract_title"],
            contract_version  = contract["contract_version"],
            table_name        = contract["table_name"],
            scan_ts           = scan_ts,
            data_source       = config.data_source,
            run_id            = run_id,
            dag_id            = effective_dag_id,
            airflow_run_id    = airflow_run_id,
            row_count_total   = total_rows,
            watermark_column  = effective_watermark_column,
            per_check_wm_from = per_check_wm,
            wm_to             = scan_ts if per_check_wm else None,
        )
        all_rows.extend(rows)
        _log_contract_summary(soda_checks, contract["contract_title"])
    else:
        logger.warning("Nessun check restituito dallo scan Soda.")

    if all_rows:
        df_results = spark.createDataFrame(all_rows, schema=RESULTS_SCHEMA)
        _log_results_summary(df_results)
        _write_results_to_iceberg(spark, df_results, config)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()
