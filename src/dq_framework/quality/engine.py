from __future__ import annotations

import logging

import json
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

from dq_framework.common.config import AppConfig
from .contract_parser import parse_contract_file
from .result_writer import RESULTS_SCHEMA, new_run_id, process_scan_results
from .soda_executor import run_dataframe_soda_scan

# Mappatura chiavi primarie importata esternamente
from dq_framework.common.config.dataset_mapping import DATASET_PK_MAP

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


# =========================================================================
# TABELLA 1: RESULTS (Log granulare delle esecuzioni)
# =========================================================================
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
            f"results_write_enabled=False (env={config.env}): scrittura su DB saltata per tabella results."
        )
        return

    fqn = f"{config.results_database}.{config.results_table}"
    logger.info(f"Scrittura risultati su tabella Iceberg principale: {fqn}")
    try:
        _ensure_results_table(spark, config, fqn)
        df_results.writeTo(fqn).append()
        logger.info(f"Scrittura completata: {df_results.count()} record inseriti in {fqn}.")
    except Exception as e:
        logger.error(f"Errore in scrittura su {fqn}: {e}", exc_info=True)
        raise


# =========================================================================
# TABELLA 2: FAILED RECORDS (Dettaglio record scartati)
# =========================================================================
_FAILED_RECORDS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
    run_id                 STRING  NOT NULL,
    check_name             STRING  NOT NULL,
    dataset                STRING  NOT NULL,
    execution_ts           TIMESTAMP NOT NULL,
    execution_date         DATE    NOT NULL,
    failed_record_pk       STRING  NOT NULL,
    failed_value           STRING
)
USING iceberg
PARTITIONED BY (execution_date)
{location_clause}
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.parquet.compression-codec'='snappy',
    'history.expire.max-snapshot-age-ms'='7776000000'
)
""".strip()

def _ensure_failed_records_table(spark: SparkSession, config: AppConfig, fqn: str) -> None:
    failed_loc_clause = ""
    if config.results_table_location:
        base_loc = config.results_table_location.rsplit("/", 1)[0]
        failed_loc_clause = f"LOCATION '{base_loc}/dqf_gpd_failed_records'"
        
    ddl = _FAILED_RECORDS_TABLE_DDL.format(fqn=fqn, location_clause=failed_loc_clause)
    spark.sql(ddl)


def _get_nested_value(record, path):
    """Recupera campi annidati (es: 'after.status') navigando dizionari e oggetti PySpark Row."""
    if record is None:
        return None
    parts = path.split('.')
    val = record
    for p in parts:
        # Se incappiamo in una Row di PySpark, la convertiamo in dict per sicurezza
        if hasattr(val, "asDict"):
            val = val.asDict()
            
        if isinstance(val, dict):
            val = val.get(p)
        else:
            return None
    return val


def _process_and_write_failed_records(spark: SparkSession, rows: list[Row], sampler, config: AppConfig):
    """Converte i campioni in RAM dal MemorySampler in un DataFrame e li salva su Iceberg."""
    if not config.results_write_enabled:
        return

    fqn = f"{config.results_database}.dqf_gpd_failed_records"
    _ensure_failed_records_table(spark, config, fqn)

    failed_rows_list = []
    
    for row in rows:
        if row.has_failed_records and row.check_name in sampler.failed_data:
            records_in_ram = sampler.failed_data[row.check_name]
            pk_cols = DATASET_PK_MAP.get(row.dataset, ["id"])
            
            for rec in records_in_ram:
                # 1. Serializzazione JSON Primary Key
                pk_dict = {pk: _get_nested_value(rec, pk) for pk in pk_cols}
                failed_record_pk = json.dumps(pk_dict, default=str)
                
                # 2. Serializzazione JSON del campo incriminato (gestione nested after_xxx -> after.xxx)
                # 2. Serializzazione JSON del campo incriminato
                failed_value = None
                if row.check_column:
                    # Logica esplicita per i field check (fld__)
                    col_name = row.check_column
                    val = _get_nested_value(rec, col_name)
                    
                    if val is None and "_" in col_name:
                        nested_col = col_name.replace("_", ".", 1) 
                        nested_val = _get_nested_value(rec, nested_col)
                        if nested_val is not None:
                            col_name = nested_col
                            val = nested_val
                            
                    failed_value = json.dumps({col_name: val}, default=str)
                else:
                    # Gestione DINAMICA per ent__ e xref__
                    # Preleviamo tutte le colonne restituite dalla fail query ESCLUSE le Primary Key
                    offending_fields = {
                        k: v for k, v in rec.items() 
                        if k not in pk_cols
                    }
                    
                    # Salviamo nel JSON solo se l'utente ha fatto una SELECT mirata (max 10 campi)
                    # per rispettare il vincolo di NON tracciare l'intero record (SELECT *)
                    if offending_fields and len(offending_fields) <= 10:
                        failed_value = json.dumps(offending_fields, default=str)
                    
                # 3. Creazione riga piatta PySpark
                failed_rows_list.append(Row(
                    run_id=row.run_id,
                    check_name=row.check_name,
                    dataset=row.dataset,
                    execution_ts=row.execution_ts,
                    execution_date=row.execution_date,
                    failed_record_pk=failed_record_pk,
                    failed_value=failed_value
                ))

    if failed_rows_list:
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("check_name", StringType(), False),
            StructField("dataset", StringType(), False),
            StructField("execution_ts", TimestampType(), False),
            StructField("execution_date", DateType(), False),
            StructField("failed_record_pk", StringType(), False),
            StructField("failed_value", StringType(), True),
        ])
        
        df_failed = spark.createDataFrame(failed_rows_list, schema=schema)

        #print(df_failed.show(100, truncate=False))

        logger.info(f"Scrittura di {df_failed.count()} record di dettaglio fallimenti su tabella Iceberg operazionale: {fqn}")
        try:
            df_failed.writeTo(fqn).append()
        except Exception as e:
            logger.error(f"Errore scrittura tabella failed records {fqn}: {e}", exc_info=True)

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

    # 1. Esecuzione tramite Soda / PySpark (ora intercettiamo anche il Sampler in memoria)
    logger.info(f"Esecuzione run_dataframe_soda_scan tramite Soda / PySpark")
    soda_checks, total_rows, sampler = run_dataframe_soda_scan(spark, contract, config)

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

        # --- INIZIO TAMPONE TEMPORANEO ---
        from pyspark.sql.functions import lit
        # Aggiungiamo dinamicamente TUTTE le colonne mancanti aggiunte dal collega
        df_results = (df_results
            .withColumn("watermark_column", lit(None).cast("string"))
            .withColumn("watermark_from", lit(None).cast("timestamp"))
            .withColumn("watermark_to", lit(None).cast("timestamp"))
        )
        # --- FINE TAMPONE TEMPORANEO ---

        _log_results_summary(df_results)
        
        # Scrittura su DB 1: Tabella aggregata (Results)
        _write_results_to_iceberg(spark, df_results, config)
        
        # Scrittura su DB 2: Tabella operativa di dettaglio (Failed records) intercettati
        _process_and_write_failed_records(spark, all_rows, sampler, config)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()