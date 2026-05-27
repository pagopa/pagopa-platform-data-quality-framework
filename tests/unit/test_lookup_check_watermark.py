"""Test della funzione _lookup_check_watermark.

Verifica:
    - query SQL emessa contiene il filtro outcome='pass' e watermark_to IS NOT NULL
    - eccezioni sulla query sono catturate e restituiscono None (bootstrap)
    - risultato None quando MAX(watermark_to) e' NULL
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock

from dq_framework.common.config.base import AppConfig
from dq_framework.quality.engine import _lookup_check_watermark


def _make_config() -> AppConfig:
    return AppConfig(
        env="test",
        github_api_base_url="https://api.github.com",
        contract_path="",
        default_repository="",
        default_ref="main",
        soda_host="cloud.soda.io",
        data_source="ds_test",
        table_limit=0,
        results_database="db_test",
        results_table="dqf_results",
    )


def _spark_returning(value):
    """Mock SparkSession il cui spark.sql(...).collect() restituisce [Row(wm=value)]."""
    fake_row = MagicMock()
    fake_row.__getitem__.side_effect = lambda k: value if k == "wm" else None

    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [fake_row]
    return spark


def _spark_raising(exc: Exception):
    spark = MagicMock()
    spark.sql.side_effect = exc
    return spark


def test_restituisce_il_max_watermark_quando_presente():
    cfg = _make_config()
    wm = datetime(2026, 5, 26, 0, 0, 0)
    spark = _spark_returning(wm)

    result = _lookup_check_watermark(spark, cfg, "silver_t", "check_a")

    assert result == wm


def test_restituisce_none_se_il_max_e_null():
    cfg = _make_config()
    spark = _spark_returning(None)

    result = _lookup_check_watermark(spark, cfg, "silver_t", "check_a")

    assert result is None


def test_eccezione_sulla_query_torna_none_senza_propagare():
    """Se la tabella results non esiste, il framework deve fallback a bootstrap."""
    cfg = _make_config()
    spark = _spark_raising(RuntimeError("Table not found: db_test.dqf_results"))

    result = _lookup_check_watermark(spark, cfg, "silver_t", "check_a")

    assert result is None


def test_query_filtra_pass_e_watermark_to_not_null():
    """La query SQL deve filtrare check_name, outcome='pass' e watermark_to IS NOT NULL."""
    cfg = _make_config()
    spark = _spark_returning(datetime(2026, 5, 26, 0, 0, 0))

    _lookup_check_watermark(spark, cfg, "silver_t", "check_a")

    sql_emitted = spark.sql.call_args[0][0]
    # Sanity checks sulla forma della query
    assert "db_test.dqf_results" in sql_emitted
    assert "check_name = 'check_a'" in sql_emitted
    assert "dataset = 'silver_t'" in sql_emitted
    assert "outcome = 'pass'" in sql_emitted
    assert "watermark_to IS NOT NULL" in sql_emitted
