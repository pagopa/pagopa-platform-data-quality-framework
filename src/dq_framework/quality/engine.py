from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from dq_framework.common.config import AppConfig
from .contract_parser import parse_contract_file
from .result_writer import RESULTS_SCHEMA, new_run_id, process_scan_results
from .soda_executor import run_dataframe_soda_scan

logger = logging.getLogger(__name__)


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
    has_failed_records     BOOLEAN NOT NULL
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
    contract_path:  str,
    repository:     str,
    ref:            str,
    config:         AppConfig,
    dag_id:         Optional[str] = None,
    airflow_run_id: Optional[str] = None,
) -> None:
    source_desc = f"{repository}@{ref}:{contract_path}" if repository else contract_path
    logger.info(f"Avvio pipeline Data Quality GPD per: {source_desc}")

    contract = parse_contract_file(contract_path, repository, ref, config)
    if not contract:
        logger.error("Contract non valido o non trovato. Pipeline terminata.")
        return

    spark    = init_spark()
    run_id   = new_run_id()
    scan_ts  = datetime.utcnow()
    # dag_id è NOT NULL in tabella: fallback se non fornito né via CLI né via env
    effective_dag_id = dag_id or f"manual:{config.env}"
    all_rows = []

    logger.info("+" * 80)
    logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")
    logger.info(f"run_id={run_id} dag_id={effective_dag_id} airflow_run_id={airflow_run_id}")

    logger.info(f"Esecuzione run_dataframe_soda_scan tramite Soda / PySpark")
    # 1. Esecuzione tramite Soda / PySpark
    soda_checks, total_rows = run_dataframe_soda_scan(spark, contract, config)

    if soda_checks:
        rows = process_scan_results(
            scan_checks      = soda_checks,
            contract_title   = contract["contract_title"],
            contract_version = contract["contract_version"],
            table_name       = contract["table_name"],
            scan_ts          = scan_ts,
            data_source      = config.data_source,
            run_id           = run_id,
            dag_id           = effective_dag_id,
            airflow_run_id   = airflow_run_id,
            row_count_total  = total_rows,
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
