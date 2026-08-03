"""Controlli incrementali: risoluzione del watermark e sostituzione del
placeholder ``${INCREMENTAL_CONDITIONS}`` nel SodaCL.

È logica di runtime (richiede una SparkSession viva e legge la tabella Iceberg
dei results), perciò vive qui e non nel contract parser. `run_pipeline` la usa
tramite l'unico entry pubblico `apply_incremental_conditions`.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import yaml
from pyspark.sql import SparkSession

from dq_framework.common.config import AppConfig

logger = logging.getLogger(__name__)

# Placeholder incrementale, nudo ``${INCREMENTAL_CONDITIONS}`` o qualificato con
# un alias di tabella ``${INCREMENTAL_CONDITIONS:spo}`` (gruppo 1 = alias).
# L'alias serve nelle query con JOIN dove la colonna watermark è ambigua; deve
# riferirsi alla tabella DRIVING della slice. Un alias malformato non matcha,
# così il token resta non sostituito e fallisce rumorosamente allo scan.
_INCREMENTAL_RE = re.compile(
    r"\$\{INCREMENTAL_CONDITIONS(?::([A-Za-z_][A-Za-z0-9_]*))?\}"
)

# Policy di avanzamento watermark -> insieme di outcome che "committano" il
# watermark nel lookup su Iceberg. Vedi AppConfig.incremental_watermark_advance_policy.
#   - pass_only: solo le run 'pass' fanno avanzare (warn/fail riprocessano la finestra)
#   - executed : ogni check eseguito (pass/warn/fail) fa avanzare; resta indietro solo
#                un check non eseguito (errore -> nessun outcome valido)
_ADVANCE_OUTCOMES: dict[str, tuple[str, ...]] = {
    "pass_only": ("pass",),
    "executed":  ("pass", "warn", "fail"),
}


def _resolve_advance_outcomes(config: AppConfig) -> tuple[str, ...]:
    """Outcome che fanno avanzare il watermark, secondo la policy configurata.

    Solleva ValueError (fail-fast) se la policy non e' riconosciuta, così un
    refuso in config non degrada silenziosamente al comportamento di default.
    """
    policy = config.incremental_watermark_advance_policy
    try:
        return _ADVANCE_OUTCOMES[policy]
    except KeyError:
        raise ValueError(
            f"incremental_watermark_advance_policy={policy!r} non valido. "
            f"Valori ammessi: {sorted(_ADVANCE_OUTCOMES)}."
        )


def _lookup_check_watermark(
    spark: SparkSession,
    config: AppConfig,
    dataset: str,
    check_name: str,
    domain: str,
    dl_layer: str,
    advance_outcomes: tuple[str, ...] = ("pass",),
) -> Optional[datetime]:
    """Restituisce il massimo `watermark_to` registrato per il check specificato.

    Filtra sugli `outcome` in `advance_outcomes` (derivati dalla policy
    `AppConfig.incremental_watermark_advance_policy`) e su `watermark_to IS NOT NULL`.
    Con `pass_only` -> ('pass',) le run in warn/fail non avanzano il watermark;
    con `executed` -> ('pass','warn','fail') anche warn/fail lo fanno avanzare,
    mentre i check non eseguiti (nessun outcome valido) restano comunque esclusi.

    Il lookup avviene sulla tabella `{dl_layer}_dqf_{domain}_results`, la stessa
    su cui scrive `result_writer`: due `dl_layer` distinti (es. silver e gold)
    hanno quindi storici e watermark completamente indipendenti.

    Se la query gira ma non trova righe utili ritorna None e il chiamante applica
    il bootstrap all'epoch. Se invece la query stessa fallisce (tabella assente,
    permessi, schema incompatibile) NON c'e' fallback: solleva RuntimeError, cosi'
    un rename/una tabella mancante non si traduce in un silenzioso riprocessamento
    integrale. In quel caso l'operatore deve passare --watermark-from.
    """
    outcome_in = ", ".join(f"'{o}'" for o in advance_outcomes)
    fqn = f"{config.results_database}.{dl_layer}_dqf_{domain}_results"
    try:
        row = spark.sql(
            f"""
            SELECT MAX(watermark_to) AS wm
            FROM {fqn}
            WHERE dataset = '{dataset}'
              AND check_name = '{check_name}'
              AND outcome IN ({outcome_in})
              AND watermark_to IS NOT NULL
            """
        ).collect()
        return row[0]["wm"] if row and row[0]["wm"] else None
    except Exception as e:
        logger.error(f"Errore critico durante il lookup del watermark su {fqn}: {e}")
        raise RuntimeError(
            f"Impossibile leggere dalla tabella {fqn} e parametro --watermark-from mancante"
        ) from e


def _resolve_ts_literal_kind(
    spark: SparkSession,
    dataset: str,
    watermark_column: str,
) -> str:
    """Variante del literal timestamp (TIMESTAMP / TIMESTAMP_NTZ) allineata al tipo
    Spark della colonna watermark. Necessaria al pushdown Iceberg: un literal di
    variante sbagliata fa inserire un cast che disabilita il partition pruning."""
    if spark is None:
        return "TIMESTAMP"
    try:
        safe_ds = ".".join(f"`{p}`" for p in dataset.split("."))
        simple = spark.table(safe_ds).schema[watermark_column].dataType.simpleString()
    except Exception as e:
        logger.warning(
            f"Tipo della colonna watermark '{watermark_column}' non risolvibile "
            f"su '{dataset}' ({e}); uso il literal TIMESTAMP (LTZ) di default."
        )
        return "TIMESTAMP"

    if simple == "timestamp_ntz":
        return "TIMESTAMP_NTZ"
    if simple == "timestamp":
        return "TIMESTAMP"

    logger.warning(
        f"Colonna watermark '{watermark_column}' su '{dataset}' ha tipo inatteso "
        f"'{simple}' (atteso timestamp/timestamp_ntz); uso TIMESTAMP di default, "
        f"verificare il partition pruning sul piano fisico."
    )
    return "TIMESTAMP"


def _build_incremental_conditions(
    watermark_column: str,
    wm_from: datetime,
    wm_to: datetime,
    alias: Optional[str] = None,
    ts_kind: str = "TIMESTAMP",
) -> str:
    """Genera la clausola SQL del placeholder: estremo sinistro escluso, destro incluso.

    Con `alias` la colonna è qualificata (`<alias>.<col>`), indispensabile nelle query
    con JOIN. `ts_kind` ("TIMESTAMP"/"TIMESTAMP_NTZ") deve combaciare col tipo della
    colonna (vedi `_resolve_ts_literal_kind`) per non perdere il pushdown Iceberg.
    """
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    col = f"{alias}.{watermark_column}" if alias else watermark_column
    return (
        f"{col} > {ts_kind} '{wm_from.strftime(fmt)}' "
        f"AND {col} <= {ts_kind} '{wm_to.strftime(fmt)}'"
    )


def _incremental_fields(check_body: dict) -> list[str]:
    """Campi del check che contengono il placeholder: la chiave ``* query`` per gli
    SQL custom, la clausola ``filter:`` per i check nativi Soda. Usa la regex (non un
    substring match) così i placeholder aliased non vengono scambiati per massivi."""
    return [
        k
        for k, v in check_body.items()
        if isinstance(k, str)
        and isinstance(v, str)
        and _INCREMENTAL_RE.search(v)
        and (k == "filter" or k.endswith(" query"))
    ]


def _resolve_per_check_watermarks(
    spark: SparkSession,
    config: AppConfig,
    contract: dict,
    scan_ts: datetime,
    watermark_column: str,
    cli_override: Optional[datetime],
    domain: str,
    dl_layer: str,
) -> tuple[str, dict[str, datetime]]:
    """Walk del SodaCL con sostituzione per-check del placeholder.

    Ritorna `(sodacl_yaml, per_check_wm)`: il secondo mappa `check_name -> wm_from`
    solo per i check incrementali (i massivi non compaiono). Risoluzione di wm_from:
    CLI override → lookup Iceberg (outcome filtrati per policy) con lookback →
    bootstrap a epoch.
    Solleva ValueError se per qualche check `wm_from >= scan_ts`.

    NB invariante naive-UTC: `scan_ts` e i watermark letti da Iceberg sono naive;
    non introdurre datetime aware o il confronto wm_from >= scan_ts esplode.
    """
    spec_dict = yaml.safe_load(contract["sodacl"])
    per_check_wm: dict[str, datetime] = {}

    if not isinstance(spec_dict, dict):
        return contract["sodacl"], per_check_wm

    ts_kind = _resolve_ts_literal_kind(spark, contract["dataset"], watermark_column)
    advance_outcomes = _resolve_advance_outcomes(config)

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

                target_fields = _incremental_fields(check_body)
                if not target_fields:
                    # Check massivo (nessun placeholder): non lo tocchiamo
                    continue

                # Risoluzione wm_from per questo specifico check
                if cli_override is not None:
                    wm_from = cli_override
                    source = "cli"
                else:
                    looked_up = _lookup_check_watermark(
                        spark, config, contract["table_name"], check_name, domain, dl_layer,
                        advance_outcomes,
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

                # re.sub con callback: ogni occorrenza risolve il proprio alias
                # condividendo lo stesso wm_from (legato come default-arg per
                # evitare il late-binding del loop).
                def _sub(m, _wm_from=wm_from, _ts_kind=ts_kind):
                    return _build_incremental_conditions(
                        watermark_column, _wm_from, scan_ts, m.group(1), _ts_kind
                    )

                for field in target_fields:
                    value = check_body[field]
                    # Advisory: placeholder NUDO in una query con JOIN è quasi sempre
                    # un errore di colonna ambigua; non indoviniamo l'alias.
                    if (" join " in value.lower()
                            and "${INCREMENTAL_CONDITIONS}" in value):
                        logger.warning(
                            f"Check '{check_name}', campo '{field}': placeholder "
                            f"${{INCREMENTAL_CONDITIONS}} nudo in una query con "
                            f"JOIN; usare ${{INCREMENTAL_CONDITIONS:<alias>}} per "
                            f"qualificare la colonna con la tabella driving."
                        )
                    check_body[field] = _INCREMENTAL_RE.sub(_sub, value)
                per_check_wm[check_name] = wm_from

                logger.debug(
                    f"Watermark check='{check_name}' "
                    f"column={watermark_column} "
                    f"from={wm_from.isoformat()} to={scan_ts.isoformat()} "
                    f"source={source}"
                )

    sodacl_yaml = yaml.safe_dump(spec_dict, sort_keys=False, allow_unicode=True)
    return sodacl_yaml, per_check_wm


def apply_incremental_conditions(
    spark: SparkSession,
    config: AppConfig,
    contract: dict,
    scan_ts: datetime,
    watermark_column_override: Optional[str],
    watermark_from_override: Optional[datetime],
    domain: str,
    dl_layer: str,
) -> tuple[str, dict[str, datetime], Optional[str]]:
    """Entry pubblico: se il SodaCL contiene il placeholder, risolve i watermark
    per-check e lo sostituisce; altrimenti è un no-op.

    Ritorna `(sodacl, per_check_wm, effective_watermark_column)`. Senza placeholder
    ritorna `(contract['sodacl'], {}, None)`. Solleva ValueError se il placeholder
    è presente ma nessuna colonna watermark è risolvibile.
    """
    if not _INCREMENTAL_RE.search(contract["sodacl"]):
        return contract["sodacl"], {}, None

    effective_watermark_column = watermark_column_override or config.default_watermark_column
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
        spark            = spark,
        config           = config,
        contract         = contract,
        scan_ts          = scan_ts,
        watermark_column = effective_watermark_column,
        cli_override     = watermark_from_override,
        domain           = domain,
        dl_layer         = dl_layer,
    )
    return new_sodacl, per_check_wm, effective_watermark_column
