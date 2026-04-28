from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import SparkSession

from dq_framework.src.common.config import AppConfig
from .contract_parser import parse_contract_file
from .result_writer import RESULTS_SCHEMA, process_scan_results
from .soda_executor import run_dataframe_soda_scan
from .impala_executor import run_impala_soda_scan

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


def _log_results_summary(df_results) -> None:
    logger.info("=" * 80)
    logger.info("RISULTATI FINALI PIPELINE DATA QUALITY")
    logger.info("=" * 80)

    for row in df_results.collect():
        outcome_tag = f"[{row.esito.upper()}]"
        logger.info(f"{outcome_tag} {row.nome_check}")
        logger.info(f"    - Contract : {row.data_contract} v{row.data_contract_version}")
        logger.info(f"    - Dataset  : {row.dataset} (Datasource: {row.datasource})")
        logger.info(f"    - Misura   : Valore={row.valore_misurato} | Righe={row.num_righe_controllate}")

    logger.info("=" * 80)


def run_pipeline(
    contract_path: str,
    repository: str,
    ref: str,
    config: AppConfig,
) -> None:
    source_desc = f"{repository}@{ref}:{contract_path}" if repository else contract_path
    logger.info(f"Avvio pipeline Data Quality GPD per: {source_desc}")

    contract = parse_contract_file(contract_path, repository, ref, config)
    if not contract:
        logger.error("Contract non valido o non trovato. Pipeline terminata.")
        return

    spark    = init_spark()
    all_rows = []

    logger.info("+" * 80)
    logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")

    logger.info(f"Esecuzione run_dataframe_soda_scan tramite Soda / PySpark")
    # 1. Esecuzione tramite Soda / PySpark
    soda_checks = run_dataframe_soda_scan(spark, contract, config)
    
    logger.info(f"Esecuzione run_impala_soda_scan su Impala")
    # 2. Esecuzione diretta su Impala tramite le query in "quality:"
    impala_checks = run_impala_soda_scan(spark, contract, config)
    
    # 3. Unione dei risultati
    all_combined_checks = soda_checks + impala_checks

    if all_combined_checks:
        # Usa la lista combinata per scrivere i risultati
        rows = process_scan_results(
            scan_checks      = all_combined_checks,
            contract_title   = contract["contract_title"],
            contract_version = contract["contract_version"],
            table_name       = contract["table_name"],
            scan_ts          = datetime.utcnow(),
            data_source      = config.data_source,
        )
        all_rows.extend(rows)
        _log_contract_summary(all_combined_checks, contract["contract_title"])
    else:
        logger.warning("Nessun check restituito dallo scan o da Impala.")

    if all_rows:
        df_results = spark.createDataFrame(all_rows, schema=RESULTS_SCHEMA)
        _log_results_summary(df_results)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()
