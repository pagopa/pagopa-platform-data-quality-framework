from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

RESULTS_SCHEMA = StructType([
    StructField("data_contract",         StringType(),    True),
    StructField("data_contract_version", StringType(),    True),
    StructField("nome_check",            StringType(),    True),
    StructField("esito",                 StringType(),    True),
    StructField("valore_misurato",       DoubleType(),    True),
    StructField("soglia_warn",           StringType(),    True),
    StructField("soglia_fail",           StringType(),    True),
    StructField("timestamp",             TimestampType(), True),
    StructField("datasource",            StringType(),    True),
    StructField("dataset",               StringType(),    True),
    StructField("num_righe_controllate", LongType(),      True),
])


def _extract_row_count(checks: list[dict]) -> Optional[int]:
    """Cerca il valore del row_count nei check processati."""
    for chk in checks:
        metrics = chk.get("metrics", []) or []
        if any("row_count" in str(m) for m in metrics):
            val = (chk.get("diagnostics") or {}).get("value")
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass
    return None


def process_scan_results(
    scan_checks:      list[dict],
    contract_title:   str,
    contract_version: str,
    table_name:       str,
    scan_ts:          datetime,
    data_source:      str,
) -> list[Row]:
    """Elabora i risultati dello scan Soda trasformandoli in Row PySpark."""
    row_count = _extract_row_count(scan_checks)
    rows: list[Row] = []

    for chk in scan_checks:
        diag  = chk.get("diagnostics") or {}
        esito = chk.get("outcome", "")

        try:
            measured = float(diag.get("value")) if diag.get("value") is not None else None
        except (TypeError, ValueError):
            measured = None

        warn_dict  = diag.get("warn")
        fail_dict  = diag.get("fail")
        check_name = chk.get("name") or chk.get("definition", "")

        rows.append(Row(
            data_contract         = contract_title,
            data_contract_version = contract_version,
            nome_check            = check_name[:80],
            esito                 = esito,
            valore_misurato       = measured,
            soglia_warn           = str(warn_dict) if warn_dict else None,
            soglia_fail           = str(fail_dict) if fail_dict else None,
            timestamp             = scan_ts,
            datasource            = chk.get("dataSource", data_source),
            dataset               = chk.get("table", table_name),
            num_righe_controllate = row_count,
        ))

    return rows
