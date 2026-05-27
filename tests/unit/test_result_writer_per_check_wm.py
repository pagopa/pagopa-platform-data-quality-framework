"""Test di process_scan_results con watermark per-check.

Verifica:
    - check massivi (assenti da per_check_wm_from) hanno le 3 colonne wm NULL
    - check incrementali hanno watermark_column, watermark_from, watermark_to popolati
    - mix di check massivi e incrementali nello stesso run produce righe coerenti
"""
from __future__ import annotations

from datetime import datetime

from dq_framework.quality.result_writer import process_scan_results


def _check_dict(name: str, outcome: str = "pass") -> dict:
    return {
        "name": name,
        "outcome": outcome,
        "diagnostics": {"value": 0},
    }


def test_check_massivo_ha_tutte_le_colonne_watermark_nulle():
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    rows = process_scan_results(
        scan_checks       = [_check_dict("check_massivo")],
        contract_title    = "T",
        contract_version  = "0.0.1",
        table_name        = "silver_t",
        scan_ts           = scan_ts,
        data_source       = "ds",
        run_id            = "r1",
        dag_id            = "manual:test",
        airflow_run_id    = None,
        row_count_total   = 100,
        watermark_column  = None,
        per_check_wm_from = {},
        wm_to             = None,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.watermark_column is None
    assert row.watermark_from is None
    assert row.watermark_to is None


def test_check_incrementale_popola_le_tre_colonne_watermark():
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    wm_from = datetime(2026, 5, 26, 0, 0, 0)
    rows = process_scan_results(
        scan_checks       = [_check_dict("check_incrementale")],
        contract_title    = "T",
        contract_version  = "0.0.1",
        table_name        = "silver_t",
        scan_ts           = scan_ts,
        data_source       = "ds",
        run_id            = "r1",
        dag_id            = "manual:test",
        airflow_run_id    = None,
        row_count_total   = 100,
        watermark_column  = "dl_event_tms",
        per_check_wm_from = {"check_incrementale": wm_from},
        wm_to             = scan_ts,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.watermark_column == "dl_event_tms"
    assert row.watermark_from   == wm_from
    assert row.watermark_to     == scan_ts


def test_mix_massivi_e_incrementali_nello_stesso_run():
    """Verifica che ogni check riceva il proprio wm o NULL in base alla mappa."""
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    wm_a    = datetime(2026, 5, 26, 0, 0, 0)
    wm_b    = datetime(2026, 5, 25, 0, 0, 0)

    rows = process_scan_results(
        scan_checks = [
            _check_dict("inc_a"),
            _check_dict("inc_b"),
            _check_dict("massivo"),
        ],
        contract_title    = "T",
        contract_version  = "0.0.1",
        table_name        = "silver_t",
        scan_ts           = scan_ts,
        data_source       = "ds",
        run_id            = "r1",
        dag_id            = "manual:test",
        airflow_run_id    = None,
        row_count_total   = 100,
        watermark_column  = "dl_event_tms",
        per_check_wm_from = {"inc_a": wm_a, "inc_b": wm_b},
        wm_to             = scan_ts,
    )

    by_name = {r.check_name: r for r in rows}

    # I due incrementali hanno wm propri
    assert by_name["inc_a"].watermark_column == "dl_event_tms"
    assert by_name["inc_a"].watermark_from   == wm_a
    assert by_name["inc_a"].watermark_to     == scan_ts

    assert by_name["inc_b"].watermark_column == "dl_event_tms"
    assert by_name["inc_b"].watermark_from   == wm_b
    assert by_name["inc_b"].watermark_to     == scan_ts

    # Il massivo ha tutte e tre NULL
    assert by_name["massivo"].watermark_column is None
    assert by_name["massivo"].watermark_from   is None
    assert by_name["massivo"].watermark_to     is None


def test_per_check_wm_from_none_equivale_a_dict_vuoto():
    """Backward compat: chiamata legacy senza watermark deve funzionare."""
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    rows = process_scan_results(
        scan_checks       = [_check_dict("any")],
        contract_title    = "T",
        contract_version  = "0.0.1",
        table_name        = "silver_t",
        scan_ts           = scan_ts,
        data_source       = "ds",
        run_id            = "r1",
        dag_id            = "manual:test",
        airflow_run_id    = None,
        row_count_total   = 100,
        # watermark_column / per_check_wm_from / wm_to non passati
    )

    assert rows[0].watermark_column is None
    assert rows[0].watermark_from is None
    assert rows[0].watermark_to is None
