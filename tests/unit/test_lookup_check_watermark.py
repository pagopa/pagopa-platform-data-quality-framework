"""Test della funzione _lookup_check_watermark e della risoluzione della policy.

Verifica:
    - query SQL emessa filtra dataset/check_name, gli outcome della policy e
      watermark_to IS NOT NULL
    - la FQN interrogata e' prefissata dal table_scope (isolamento per layer)
    - policy "pass_only" -> outcome IN ('pass'); "executed" -> ('pass','warn','fail')
    - eccezioni sulla query NON sono catturate: fail-fast con RuntimeError, cosi'
      una tabella assente non diventa un riprocessamento integrale silenzioso
    - risultato None quando MAX(watermark_to) e' NULL (bootstrap all'epoch)
    - _resolve_advance_outcomes mappa la policy e fa fail-fast sui valori ignoti
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dq_framework.common.config.base import AppConfig
from dq_framework.quality.utils.incremental import (
    _lookup_check_watermark,
    _resolve_advance_outcomes,
)

DOMAIN = "gpd"
SCOPE = "silver"


def _make_config(**overrides) -> AppConfig:
    base = AppConfig(
        env="test",
        github_api_base_url="https://api.github.com",
        contract_path="",
        default_repository="",
        default_ref="main",
        soda_host="cloud.soda.io",
        data_source="ds_test",
        table_limit=0,
        results_database="db_test",
    )
    return replace(base, **overrides)


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

    result = _lookup_check_watermark(spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE)

    assert result == wm


def test_restituisce_none_se_il_max_e_null():
    cfg = _make_config()
    spark = _spark_returning(None)

    result = _lookup_check_watermark(spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE)

    assert result is None


def test_eccezione_sulla_query_propaga_runtime_error():
    """Se il lookup fallisce (tabella assente, tipicamente dopo un rename) NON c'e'
    fallback a bootstrap: fail-fast, altrimenti si riprocesserebbe in silenzio
    l'intero storico. L'operatore deve passare --watermark-from."""
    cfg = _make_config()
    spark = _spark_raising(RuntimeError("Table not found: db_test.silver_dqf_gpd_results"))

    with pytest.raises(RuntimeError, match="--watermark-from"):
        _lookup_check_watermark(spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE)


def test_query_forma_base_filtra_dataset_check_e_watermark_not_null():
    """La query SQL deve filtrare la FQN per scope e dominio, check_name, dataset e
    watermark_to IS NOT NULL."""
    cfg = _make_config()
    spark = _spark_returning(datetime(2026, 5, 26, 0, 0, 0))

    _lookup_check_watermark(spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE)

    sql_emitted = spark.sql.call_args[0][0]
    assert "db_test.silver_dqf_gpd_results" in sql_emitted
    assert "check_name = 'check_a'" in sql_emitted
    assert "dataset = 'silver_t'" in sql_emitted
    assert "watermark_to IS NOT NULL" in sql_emitted


def test_table_scope_cambia_la_tabella_interrogata():
    """Il table_scope prefissa la FQN del lookup: due scope diversi leggono da
    tabelle diverse, quindi i watermark dei layer restano indipendenti."""
    cfg = _make_config()
    spark = _spark_returning(datetime(2026, 5, 26, 0, 0, 0))

    _lookup_check_watermark(spark, cfg, "gold_t", "check_a", DOMAIN, "gold")

    assert "db_test.gold_dqf_gpd_results" in spark.sql.call_args[0][0]


def test_default_outcomes_filtra_solo_pass():
    """Senza policy esplicita (default param) il filtro e' outcome IN ('pass')."""
    cfg = _make_config()
    spark = _spark_returning(datetime(2026, 5, 26, 0, 0, 0))

    _lookup_check_watermark(spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE)

    sql_emitted = spark.sql.call_args[0][0]
    assert "outcome IN ('pass')" in sql_emitted
    assert "warn" not in sql_emitted
    assert "fail" not in sql_emitted


def test_policy_executed_include_warn_e_fail_nel_filtro():
    """Con advance_outcomes=('pass','warn','fail') anche warn/fail avanzano."""
    cfg = _make_config()
    spark = _spark_returning(datetime(2026, 5, 26, 0, 0, 0))

    _lookup_check_watermark(
        spark, cfg, "silver_t", "check_a", DOMAIN, SCOPE,
        advance_outcomes=("pass", "warn", "fail"),
    )

    sql_emitted = spark.sql.call_args[0][0]
    assert "outcome IN ('pass', 'warn', 'fail')" in sql_emitted


# ---------------------------------------------------------------------------
# _resolve_advance_outcomes: mappa policy -> outcome, fail-fast sui valori ignoti
# ---------------------------------------------------------------------------

def test_resolve_advance_outcomes_pass_only():
    cfg = _make_config(incremental_watermark_advance_policy="pass_only")
    assert _resolve_advance_outcomes(cfg) == ("pass",)


def test_resolve_advance_outcomes_default_e_pass_only():
    """Il default di AppConfig preserva il comportamento storico."""
    assert _resolve_advance_outcomes(_make_config()) == ("pass",)


def test_resolve_advance_outcomes_executed():
    cfg = _make_config(incremental_watermark_advance_policy="executed")
    assert _resolve_advance_outcomes(cfg) == ("pass", "warn", "fail")


def test_resolve_advance_outcomes_policy_ignota_solleva():
    cfg = _make_config(incremental_watermark_advance_policy="bogus")
    with pytest.raises(ValueError, match="incremental_watermark_advance_policy"):
        _resolve_advance_outcomes(cfg)
