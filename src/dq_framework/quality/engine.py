from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession

from dq_framework.common.config import AppConfig

# Import a livello di modulo: run_pipeline chiama i bare global, così i
# monkeypatch dei test (monkeypatch.setattr(engine, ...)) si agganciano al
# punto di chiamata effettivo.
from .contract_parser import parse_contract_file, extract_and_clean_failed_queries
from .utils.incremental import apply_incremental_conditions
from .soda_executor import run_dataframe_soda_scan
from .result_writer import (
    RESULTS_SCHEMA,
    process_scan_results,
    write_results_to_iceberg,
    process_and_write_failed_records,
    run_manual_failed_queries,
)
from .utils.result_logging import log_contract_summary, log_results_summary

logger = logging.getLogger(__name__)


def init_spark(app_name: str = "gpd_quality_pipeline") -> SparkSession:
    logger.info(f"Inizializzazione SparkSession (appName={app_name})...")
    return (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )


def run_pipeline(
    contract_path:             str,
    repository:                str,
    ref:                       str,
    config:                    AppConfig,
    dag_id:                    Optional[str]       = None,
    airflow_run_id:            Optional[str]       = None,
    watermark_column_override: Optional[str]       = None,
    watermark_from_override:   Optional[datetime]  = None,
    primary_keys:              Optional[list[str]] = None,
) -> None:
    source_desc = f"{repository}@{ref}:{contract_path}" if repository else contract_path
    logger.info(f"Avvio pipeline Data Quality GPD per: {source_desc}")

    contract = parse_contract_file(contract_path, repository, ref, config)
    if not contract:
        logger.error("Contract non valido o non trovato. Pipeline terminata.")
        return

    spark   = init_spark(app_name=f"gpd_quality_{contract['table_name']}")
    run_id  = str(uuid.uuid4())
    scan_ts = datetime.now(timezone.utc).replace(tzinfo=None)  # FREEZE POINT, naive-UTC
    effective_dag_id       = dag_id or f"manual:{config.env}"
    effective_primary_keys = primary_keys or list(config.default_primary_keys)
    result_rows = []

    logger.info("+" * 80)
    logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")
    logger.info(f"run_id={run_id} dag_id={effective_dag_id} airflow_run_id={airflow_run_id}")

    # Controlli incrementali: l'intero blocco watermark collassa in una chiamata.
    contract["sodacl"], per_check_wm, effective_watermark_column = apply_incremental_conditions(
        spark, config, contract, scan_ts, watermark_column_override, watermark_from_override
    )

    # Estrazione delle failed-query DOPO la sostituzione watermark, così la query
    # differita ha già i timestamp risolti.
    contract["sodacl"], extracted_queries = extract_and_clean_failed_queries(contract["sodacl"])

    logger.info("Esecuzione run_dataframe_soda_scan tramite Soda / PySpark")
    soda_checks, total_rows, sampler = run_dataframe_soda_scan(spark, contract, config)

    if soda_checks:
        result_rows.extend(process_scan_results(
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
        ))
        log_contract_summary(soda_checks, contract["contract_title"])
    else:
        logger.warning("Nessun check restituito dallo scan Soda.")

    if result_rows:
        df_results = spark.createDataFrame(result_rows, schema=RESULTS_SCHEMA)

        log_results_summary(df_results)

        # DB 1: tabella aggregata (results)
        write_results_to_iceberg(spark, df_results, config)

        if extracted_queries:
            run_manual_failed_queries(spark, result_rows, extracted_queries, sampler, config)

        # DB 2: tabella operativa di dettaglio (failed records)
        process_and_write_failed_records(spark, result_rows, sampler, config, effective_primary_keys)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()
