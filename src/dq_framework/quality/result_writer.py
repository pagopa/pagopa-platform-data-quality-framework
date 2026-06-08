from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import Row
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


def _failed_row_count(chk: dict, diag: dict) -> Optional[int]:
    metrics = chk.get("metrics", []) or []
    check_def = chk.get("definition", "").lower()
    check_name = chk.get("name", "")
    
    is_bad_row_count = False
    
    for m in metrics:
        m_str = str(m).lower()
        if any(k in m_str for k in ("missing_count", "invalid_count", "duplicate_count", "failed_rows")):
            is_bad_row_count = True
            break

    if not is_bad_row_count and "failed rows:" in check_def:
        is_bad_row_count = True

    if is_bad_row_count:
        val = diag.get("value")
        if val is not None:
            try:
                return int(val)
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

    Le colonne watermark vengono valorizzate solo per i check incrementali,
    ossia quelli il cui `check_name` compare in `per_check_wm_from`. Per i
    check massivi le tre colonne `watermark_column`, `watermark_from` e
    `watermark_to` restano NULL. Cio' consente al lookup successivo di
    distinguere correttamente "ultima run pass incrementale" da "ultima run
    pass qualsiasi".
    """

    execution_date = scan_ts.date()
    per_check_wm_from = per_check_wm_from or {}
    rows: list[Row] = []

    for chk in scan_checks:
        diag  = chk.get("diagnostics") or {}
        outcome = chk.get("outcome", "") or ""

        raw_value = diag.get("value")
        measured_numeric = _as_numeric(raw_value)
        measured_string = _as_string(raw_value) if measured_numeric is None else None

        warn_dict = diag.get("warn")
        fail_dict = diag.get("fail")
        check_name = chk.get("name") or chk.get("definition", "") or ""

        category, dimension, column_from_name = _parse_check_name(check_name)
        check_column = chk.get("column") or column_from_name

        row_count_failed = _failed_row_count(chk, diag)
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
            dataset                = chk.get("table", table_name),
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


def new_run_id() -> str:
    return str(uuid.uuid4())
