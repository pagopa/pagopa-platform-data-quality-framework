"""Helper di logging per i risultati della pipeline (solo output, nessuna logica)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def log_contract_summary(checks: list[dict], contract_title: str) -> None:
    passed = [c for c in checks if c.get("outcome") == "pass"]
    warned = [c for c in checks if c.get("outcome") == "warn"]
    failed = [c for c in checks if c.get("outcome") == "fail"]
    errors = [c for c in checks if c.get("outcome") == "error"]

    logger.info(
        f"Riepilogo contract [{contract_title}] - "
        f"Totale: {len(checks)} | PASS: {len(passed)} | WARN: {len(warned)} | "
        f"FAIL: {len(failed)} | ERRORS: {len(errors)}"
    )

    for check in warned + failed + errors:
        diagnostics = check.get("diagnostics") or {}
        outcome = check.get("outcome", "unknown").upper()
        logger.warning(
            f"[{outcome}] Check: {check.get('name')} | Valore Rilevato: {diagnostics.get('value')}"
        )


def log_results_summary(df_results: DataFrame) -> None:
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
