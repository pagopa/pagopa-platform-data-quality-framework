"""Test di _resolve_per_check_watermarks (YAML walk + sostituzione per-check).

I test mockano `_lookup_check_watermark` per evitare dipendenze da Spark
e da una tabella Iceberg reale. Si concentrano sul comportamento del walker:
    - solo i check con placeholder vengono toccati
    - ogni check incrementale riceve la sua specifica wm_from
    - bootstrap quando il lookup torna None
    - CLI override forza wm_from per tutti i check incrementali
    - errore se wm_from >= scan_ts
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from unittest.mock import patch

import pytest
import yaml

from dq_framework.common.config.base import AppConfig
from dq_framework.quality import engine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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


def _make_contract(sodacl: str, table_name: str = "silver_t") -> dict:
    return {
        "contract_path":    "/fixture/dc.yaml",
        "contract_title":   "Fixture Contract",
        "contract_version": "0.0.1",
        "dataset":          f"pagopa.{table_name}",
        "table_name":       table_name,
        "sodacl":           sodacl,
        "impala_checks":    [],
    }


SODACL_MISTO = """
checks for silver_t:

  - inc_check_a = 0:
      name: check_a_incrementale
      inc_check_a query: |
        SELECT COUNT(*) FROM silver_t
        WHERE col_x IS NULL
          AND ${INCREMENTAL_CONDITIONS}

  - inc_check_b = 0:
      name: check_b_incrementale
      inc_check_b query: |
        SELECT COUNT(*) FROM silver_t
        WHERE ${INCREMENTAL_CONDITIONS}

  - massive_check = 0:
      name: check_massivo
      massive_check query: |
        SELECT COUNT(*) FROM silver_t WHERE col_x < 0
""".strip()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_sostituisce_solo_check_con_placeholder():
    """Il check massivo NON deve essere toccato; quelli incrementali si'."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)

    with patch.object(engine, "_lookup_check_watermark") as m:
        m.side_effect = [
            datetime(2026, 5, 26, 0, 0, 0),   # check_a
            datetime(2026, 5, 25, 0, 0, 0),   # check_b
        ]
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,    # mockato sotto, non viene chiamato
            config=cfg,
            contract=_make_contract(SODACL_MISTO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    spec = yaml.safe_load(new_sodacl)
    checks = spec["checks for silver_t"]

    # I due incrementali hanno ricevuto la sostituzione, il massivo no
    query_a = checks[0]["inc_check_a = 0"]["inc_check_a query"]
    query_b = checks[1]["inc_check_b = 0"]["inc_check_b query"]
    query_m = checks[2]["massive_check = 0"]["massive_check query"]

    assert "${INCREMENTAL_CONDITIONS}" not in query_a
    assert "${INCREMENTAL_CONDITIONS}" not in query_b
    assert "${INCREMENTAL_CONDITIONS}" not in query_m   # mai esistito
    assert "dl_event_tms >"  in query_a
    assert "dl_event_tms <=" in query_a
    assert "dl_event_tms >"  in query_b
    assert "WHERE col_x < 0" in query_m

    # Solo i due incrementali popolano per_check_wm
    assert set(per_check_wm.keys()) == {
        "check_a_incrementale",
        "check_b_incrementale",
    }


def test_ogni_check_riceve_il_suo_specifico_watermark():
    """Verifica che il lookup per-check produca substitutions diverse."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    wm_a = datetime(2026, 5, 26, 12, 0, 0)
    wm_b = datetime(2026, 5, 25, 12, 0, 0)

    with patch.object(engine, "_lookup_check_watermark") as m:
        m.side_effect = [wm_a, wm_b]
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_MISTO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    assert per_check_wm["check_a_incrementale"] == wm_a
    assert per_check_wm["check_b_incrementale"] == wm_b

    spec = yaml.safe_load(new_sodacl)
    query_a = spec["checks for silver_t"][0]["inc_check_a = 0"]["inc_check_a query"]
    query_b = spec["checks for silver_t"][1]["inc_check_b = 0"]["inc_check_b query"]

    assert "2026-05-26 12:00:00" in query_a
    assert "2026-05-25 12:00:00" in query_b


def test_bootstrap_quando_lookup_torna_none():
    """Se la tabella results e' vuota per quel check, fallback a epoch."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        _, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_MISTO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    assert per_check_wm["check_a_incrementale"] == datetime(1970, 1, 1)
    assert per_check_wm["check_b_incrementale"] == datetime(1970, 1, 1)


def test_cli_override_forza_wm_from_per_tutti_i_check_incrementali():
    cfg          = _make_config()
    scan_ts      = datetime(2026, 5, 27, 3, 0, 0)
    cli_override = datetime(2026, 5, 20, 0, 0, 0)

    with patch.object(engine, "_lookup_check_watermark") as m:
        _, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_MISTO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=cli_override,
        )

    # Con CLI override il lookup NON dovrebbe essere mai chiamato
    m.assert_not_called()
    assert per_check_wm["check_a_incrementale"] == cli_override
    assert per_check_wm["check_b_incrementale"] == cli_override


def test_lookback_minutes_sottrae_dal_valore_dell_lookup():
    cfg     = _make_config(incremental_lookback_minutes=30)
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    lookup_value = datetime(2026, 5, 26, 12, 0, 0)

    with patch.object(engine, "_lookup_check_watermark", return_value=lookup_value):
        _, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_MISTO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    # 30 minuti di lookback applicati al valore dell'Iceberg lookup
    expected = datetime(2026, 5, 26, 11, 30, 0)
    assert per_check_wm["check_a_incrementale"] == expected


def test_errore_se_wm_from_maggiore_uguale_wm_to():
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    # wm_from successivo a scan_ts -> invalid
    invalid_wm = datetime(2026, 6, 1, 0, 0, 0)

    with patch.object(engine, "_lookup_check_watermark", return_value=invalid_wm):
        with pytest.raises(ValueError, match="Watermark invalido"):
            engine._resolve_per_check_watermarks(
                spark=None,
                config=cfg,
                contract=_make_contract(SODACL_MISTO),
                scan_ts=scan_ts,
                watermark_column="dl_event_tms",
                cli_override=None,
            )


def test_contract_senza_placeholder_non_chiama_il_lookup_e_non_popola_dict():
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    sodacl_massivo = """
    checks for silver_t:
      - massive_check = 0:
          name: check_massivo
          massive_check query: |
            SELECT COUNT(*) FROM silver_t WHERE col_x < 0
    """.strip()

    with patch.object(engine, "_lookup_check_watermark") as m:
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(sodacl_massivo),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    m.assert_not_called()
    assert per_check_wm == {}
