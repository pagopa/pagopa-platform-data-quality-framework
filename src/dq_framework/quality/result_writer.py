from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_framework.common.config import AppConfig

logger = logging.getLogger(__name__)

RESULTS_SCHEMA = StructType([
    StructField("run_id",                 StringType(),    False),
    StructField("airflow_run_id",         StringType(),    True),
    StructField("dag_id",                 StringType(),    False),
    StructField("execution_ts",           TimestampType(), False),
    StructField("execution_date",         DateType(),      False),
    StructField("dataset",                StringType(),    False),
    StructField("check_name",             StringType(),    False),
    StructField("check_column",           StringType(),    True),
    StructField("check_category",         StringType(),    True),
    StructField("check_dimension",        StringType(),    True),
    StructField("outcome",                StringType(),    False),
    StructField("measured_value_numeric", DoubleType(),    True),
    StructField("measured_value_string",  StringType(),    True),
    StructField("threshold_warn",         StringType(),    True),
    StructField("threshold_fail",         StringType(),    True),
    StructField("row_count_total",        LongType(),      True),
    StructField("row_count_failed",       LongType(),      True),
    StructField("has_failed_records",     BooleanType(),   False),
    # Colonne watermark per controlli incrementali (NULL per controlli massivi)
    StructField("watermark_column",       StringType(),    True),
    StructField("watermark_from",         TimestampType(), True),
    StructField("watermark_to",           TimestampType(), True),
])

_CHECK_NAME_RE = re.compile(
    r"^(?P<prefix>fld|ent|xref)__(?P<dim>acc|cmp|cns|tim|unq|vld)__"
    r"(?P<ctx>[a-z0-9_]+)__(?P<rule>[a-z0-9_]+)$"
)

_CATEGORY_MAP = {
    "fld":  "field-level",
    "ent":  "intra-entity",
    "xref": "cross-entity",
}

_DIMENSION_MAP = {
    "acc": "accuracy",
    "cmp": "completeness",
    "cns": "consistency",
    "tim": "timeliness",
    "unq": "uniqueness",
    "vld": "validity",
}


def _parse_check_name(name: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Estrae (category, dimension, column) dal nome del check secondo naming convention."""
    if not name:
        return None, None, None
    m = _CHECK_NAME_RE.match(name)
    if not m:
        return None, None, None
    category = _CATEGORY_MAP.get(m.group("prefix"))
    dimension = _DIMENSION_MAP.get(m.group("dim"))
    column = m.group("ctx") if m.group("prefix") == "fld" else None
    return category, dimension, column


def _as_numeric(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_string(value) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _failed_row_count(check: dict, diagnostics: dict) -> Optional[int]:
    metrics = check.get("metrics", []) or []
    check_def = check.get("definition", "").lower()

    is_bad_row_count = False
    for metric in metrics:
        metric_str = str(metric).lower()
        if any(k in metric_str for k in ("missing_count", "invalid_count", "duplicate_count", "failed_rows")):
            is_bad_row_count = True
            break

    if not is_bad_row_count and "failed rows:" in check_def:
        is_bad_row_count = True

    if is_bad_row_count:
        value = diagnostics.get("value")
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass

    return None


def process_scan_results(
    scan_checks:       list[dict],
    contract_title:    str,
    contract_version:  str,
    table_name:        str,
    scan_ts:           datetime,
    data_source:       str,
    run_id:            str,
    dag_id:            str,
    airflow_run_id:    Optional[str],
    row_count_total:   int,
    watermark_column:  Optional[str]                    = None,
    per_check_wm_from: Optional[dict[str, datetime]]    = None,
    wm_to:             Optional[datetime]               = None,
) -> list[Row]:
    """Elabora i risultati dello scan Soda trasformandoli in Row PySpark
    conformi allo schema della tabella Iceberg dqf_gpd_results.

    Le colonne watermark vengono valorizzate solo per i check incrementali
    (quelli il cui `check_name` compare in `per_check_wm_from`); per i massivi
    restano NULL, così il lookup distingue "ultima run pass incrementale" da
    "ultima run pass qualsiasi".
    """
    execution_date = scan_ts.date()
    per_check_wm_from = per_check_wm_from or {}
    rows: list[Row] = []

    for check in scan_checks:
        diagnostics = check.get("diagnostics") or {}
        outcome = check.get("outcome", "") or ""

        raw_value = diagnostics.get("value")
        measured_numeric = _as_numeric(raw_value)
        measured_string = _as_string(raw_value) if measured_numeric is None else None

        warn_dict = diagnostics.get("warn")
        fail_dict = diagnostics.get("fail")
        check_name = check.get("name") or check.get("definition", "") or ""

        category, dimension, column_from_name = _parse_check_name(check_name)
        check_column = check.get("column") or column_from_name

        row_count_failed = _failed_row_count(check, diagnostics)
        has_failed_records = (
            outcome.lower() == "fail"
            or (row_count_failed is not None and row_count_failed > 0)
        )

        # Watermark per-check: valorizzato solo se il check era incrementale
        check_wm_from = per_check_wm_from.get(check_name)
        is_incremental_check = check_wm_from is not None

        rows.append(Row(
            run_id                 = run_id,
            airflow_run_id         = airflow_run_id,
            dag_id                 = dag_id,
            execution_ts           = scan_ts,
            execution_date         = execution_date,
            dataset                = check.get("table", table_name),
            check_name             = check_name,
            check_column           = check_column,
            check_category         = category,
            check_dimension        = dimension,
            outcome                = outcome,
            measured_value_numeric = measured_numeric,
            measured_value_string  = measured_string,
            threshold_warn         = str(warn_dict) if warn_dict else None,
            threshold_fail         = str(fail_dict) if fail_dict else None,
            row_count_total        = row_count_total,
            row_count_failed       = row_count_failed,
            has_failed_records     = bool(has_failed_records),
            watermark_column       = watermark_column if is_incremental_check else None,
            watermark_from         = check_wm_from,
            watermark_to           = wm_to            if is_incremental_check else None,
        ))

    return rows


# =========================================================================
# TABELLA 1: RESULTS (log granulare delle esecuzioni)
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


def _ensure_results_table(spark: SparkSession, config: AppConfig, fqn: str, domain: str) -> None:
    location_clause = ""
    if config.results_table_location:
        base_loc = config.results_table_location.rsplit("/", 1)[0]
        location_clause = f"LOCATION '{base_loc}/dqf_{domain}_results'"
        
    ddl = _RESULTS_TABLE_DDL.format(fqn=fqn, location_clause=location_clause)
    logger.info(f"Esecuzione CREATE TABLE IF NOT EXISTS su {fqn}")
    spark.sql(ddl)


def write_results_to_iceberg(spark: SparkSession, df_results: DataFrame, config: AppConfig, domain: str) -> None:
    if not config.results_write_enabled:
        logger.info(
            f"results_write_enabled=False (env={config.env}): scrittura su DB saltata per tabella results."
        )
        return

    fqn = f"{config.results_database}.dqf_{domain}_results"
    logger.info(f"Scrittura risultati su tabella Iceberg principale: {fqn}")
    try:
        _ensure_results_table(spark, config, fqn, domain)
        df_results.writeTo(fqn).append()
        logger.info(f"Scrittura completata: {df_results.count()} record inseriti in {fqn}.")
    except Exception as e:
        logger.error(f"Errore in scrittura su {fqn}: {e}", exc_info=True)
        raise


# =========================================================================
# TABELLA 2: FAILED RECORDS (dettaglio record scartati)
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


def _ensure_failed_records_table(spark: SparkSession, config: AppConfig, fqn: str, domain: str) -> None:
    failed_loc_clause = ""
    if config.results_table_location:
        base_loc = config.results_table_location.rsplit("/", 1)[0]
        failed_loc_clause = f"LOCATION '{base_loc}/dqf_{domain}_failed_records'"

    ddl = _FAILED_RECORDS_TABLE_DDL.format(fqn=fqn, location_clause=failed_loc_clause)
    spark.sql(ddl)


def run_manual_failed_queries(
    spark: SparkSession,
    result_rows: list[Row],
    extracted_queries: dict,
    sampler,
    config: AppConfig,
) -> None:
    """Per ogni check fallito con query custom, ricostruisce la SELECT dei campi
    incriminati (sostituendo COUNT(*) coi 'failed-query-fields') e la esegue,
    riversando i record nel MemorySampler per la scrittura su Iceberg.
    """
    pending = [r for r in result_rows if r.has_failed_records and r.check_name in extracted_queries]
    if not pending:
        return

    logger.info(f"Esecuzione di {len(pending)} failed query differite per il dettaglio record...")

    

    for row in pending:
        
        query_info = extracted_queries[row.check_name]
        base_query = query_info["query"]
        fields = query_info["fields"]

        # 'SELECT COUNT(*)' -> 'SELECT campo1, campo2' (case-insensitive, gestisce COUNT(1))
        failed_records_query = re.sub(
            r'(?i)^\s*SELECT\s+COUNT\s*\(\s*(\*|1)\s*\)',
            f"SELECT {fields}",
            base_query,
            count=1,
        )
        failed_records_query += f"LIMIT {config.failed_sample_limit}"

        try:
            logger.info(f"Failed query per check {row.check_name}: {failed_records_query}")

            failed_df = spark.sql(failed_records_query)
            sampler.failed_data[row.check_name] = [record.asDict() for record in failed_df.collect()]
        except Exception as e:
            logger.error(
                f"Errore nell'esecuzione della failed query differita per "
                f"'{row.check_name}': {e}\nQuery: {failed_records_query}"
            )


def _get_nested_value(record, path):
    """Recupera campi annidati gestendo in modo ricorsivo sia i dict Python che i Row PySpark."""
    if record is None:
        return None

    def _to_dict(obj):
        if hasattr(obj, "asDict"):
            return obj.asDict(recursive=True)
        elif isinstance(obj, dict):
            return {k: _to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_to_dict(v) for v in obj]
        else:
            return obj

    clean_record = _to_dict(record)
    if not isinstance(clean_record, dict):
        return None

    # 1. Match letterale esatto (es. la colonna si chiama "after.id")
    if path in clean_record:
        return clean_record[path]

    # 2. Navigazione gerarchica (es. clean_record["after"]["id"])
    parts = path.split('.')
    val = clean_record
    for part in parts:
        if isinstance(val, dict) and part in val:
            val = val[part]
        else:
            val = None
            break
    if val is not None:
        return val

    # 3. Fallback sul nome "foglia" (es. cerca "id" invece di "after.id")
    leaf_name = parts[-1]
    if leaf_name in clean_record:
        return clean_record[leaf_name]

    return None


def process_and_write_failed_records(
    spark: SparkSession,
    rows: list[Row],
    sampler,
    config: AppConfig,
    primary_keys: list[str],
    domain: str,
) -> None:
    """Converte i campioni in RAM dal MemorySampler in un DataFrame e li salva su Iceberg.

    `primary_keys` arriva dal DAG (--primary-keys); supporta chiavi composite e
    nested (es. 'after.id'). Se vuoto si usa la surrogata ['dl_id'].
    """
    if not config.results_write_enabled:
        return

    pk_cols = primary_keys or ["dl_id"]
    fqn = f"{config.results_database}.dqf_{domain}_failed_records"
    _ensure_failed_records_table(spark, config, fqn, domain)

    failed_rows_list = []

    for row in rows:
        if not (row.has_failed_records and row.check_name in sampler.failed_data):
            continue

        for record in sampler.failed_data[row.check_name]:
            # 1. Serializzazione JSON della primary key
            pk_dict = {pk: _get_nested_value(record, pk) for pk in pk_cols}
            failed_record_pk = json.dumps(pk_dict, default=str)

            # 2. Serializzazione JSON del campo incriminato
            failed_value = None
            if row.check_column:
                # Field check (fld__): risolve anche il nested after_xxx -> after.xxx
                col_name = row.check_column
                val = _get_nested_value(record, col_name)
                if val is None and "_" in col_name:
                    nested_col = col_name.replace("_", ".", 1)
                    nested_val = _get_nested_value(record, nested_col)
                    if nested_val is not None:
                        col_name = nested_col
                        val = nested_val
                failed_value = json.dumps({col_name: val}, default=str)
            else:
                # ent__ / xref__: serializza i campi non-PK (max 10, niente SELECT *)
                pk_leafs = [pk.split('.')[-1] for pk in pk_cols]
                offending_fields = {
                    k: v for k, v in record.items()
                    if k not in pk_cols and k not in pk_leafs
                }
                if offending_fields and len(offending_fields) <= 10:
                    failed_value = json.dumps(offending_fields, default=str)

            # 3. Riga piatta PySpark
            failed_rows_list.append(Row(
                run_id=row.run_id,
                check_name=row.check_name,
                dataset=row.dataset,
                execution_ts=row.execution_ts,
                execution_date=row.execution_date,
                failed_record_pk=failed_record_pk,
                failed_value=failed_value,
            ))

    if not failed_rows_list:
        return

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

    logger.info(f"Scrittura di {len(failed_rows_list)} record di dettaglio fallimenti su {fqn}")
    try:
        df_failed.writeTo(fqn).append()
    except Exception as e:
        logger.error(f"Errore scrittura tabella failed records {fqn}: {e}", exc_info=True)
