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


# Check nativi Soda (missing_count, invalid_count, ...): la condizione
# incrementale non vive in una "* query" ma nella clausola "filter:".
SODACL_NATIVO = """
checks for silver_t:

  # filter SOLO incrementale
  - missing_count(op) = 0:
      name: fld__cmp__op__not_null
      filter: ${INCREMENTAL_CONDITIONS}

  # filter combinato: condizione preesistente + incrementale concatenata
  - missing_count(ts_ms) = 0:
      name: fld__cmp__ts_ms__not_null
      filter: op IN ('c', 'r', 'u') AND ${INCREMENTAL_CONDITIONS}

  # check nativo massivo: filter senza placeholder, non va toccato
  - missing_count(ts_us) = 0:
      name: fld__cmp__ts_us__not_null
      filter: op IN ('c', 'r', 'u')
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


# ---------------------------------------------------------------------------
# Check nativi Soda con placeholder nella clausola filter
# ---------------------------------------------------------------------------

def test_sostituisce_placeholder_dentro_filter_dei_check_nativi():
    """I check nativi (missing_count) mettono l'incrementale nel `filter:`.

    - filter solo incrementale -> diventa la clausola completa
    - filter combinato -> mantiene la condizione preesistente + AND incrementale
    - filter senza placeholder -> resta invariato e non popola per_check_wm
    """
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)

    with patch.object(engine, "_lookup_check_watermark") as m:
        m.side_effect = [
            datetime(2026, 5, 26, 0, 0, 0),   # op
            datetime(2026, 5, 25, 0, 0, 0),   # ts_ms
        ]
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_NATIVO),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    spec   = yaml.safe_load(new_sodacl)
    checks = spec["checks for silver_t"]

    filter_op    = checks[0]["missing_count(op) = 0"]["filter"]
    filter_ts_ms = checks[1]["missing_count(ts_ms) = 0"]["filter"]
    filter_ts_us = checks[2]["missing_count(ts_us) = 0"]["filter"]

    # filter solo incrementale: placeholder sostituito dalla clausola completa
    assert "${INCREMENTAL_CONDITIONS}" not in filter_op
    assert "dl_event_tms >"  in filter_op
    assert "dl_event_tms <=" in filter_op

    # filter combinato: la condizione preesistente sopravvive accanto a quella incrementale
    assert "${INCREMENTAL_CONDITIONS}" not in filter_ts_ms
    assert "op IN ('c', 'r', 'u')" in filter_ts_ms
    assert "AND dl_event_tms >"     in filter_ts_ms

    # filter massivo: invariato
    assert filter_ts_us == "op IN ('c', 'r', 'u')"

    # Solo i due check nativi incrementali popolano per_check_wm
    assert set(per_check_wm.keys()) == {
        "fld__cmp__op__not_null",
        "fld__cmp__ts_ms__not_null",
    }


# ---------------------------------------------------------------------------
# Placeholder qualificato con alias (query con JOIN): ${INCREMENTAL_CONDITIONS:spo}
# ---------------------------------------------------------------------------

# xref check con anti-join: entrambe le tabelle hanno dl_event_tms, quindi la
# colonna watermark DEVE essere qualificata con l'alias della tabella driving.
SODACL_ALIASED = """
checks for silver_t:

  - failed rows:
      name: xref__cns__ref_integrity
      fail query: |
        SELECT spo.dl_id
        FROM silver_t spo
        LEFT JOIN silver_other spp
          ON spp.after.id = spo.after.ref_id
        WHERE spp.dl_id IS NULL
          AND ${INCREMENTAL_CONDITIONS:spo}
""".strip()


def test_sostituzione_aliased_produce_colonna_qualificata():
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_ALIASED),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    spec  = yaml.safe_load(new_sodacl)
    query = spec["checks for silver_t"][0]["failed rows"]["fail query"]

    assert "${INCREMENTAL_CONDITIONS" not in query
    assert "spo.dl_event_tms >"  in query
    assert "spo.dl_event_tms <=" in query
    # la colonna NON deve comparire nuda (sarebbe ambigua)
    assert " dl_event_tms >" not in query
    assert per_check_wm == {"xref__cns__ref_integrity": datetime(1970, 1, 1)}


def test_due_check_con_alias_diversi_ricevono_prefissi_distinti():
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    sodacl = """
    checks for silver_t:
      - failed rows:
          name: xref_a
          fail query: |
            SELECT spo.dl_id FROM silver_t spo JOIN o ON o.id = spo.id
            WHERE ${INCREMENTAL_CONDITIONS:spo}
      - failed rows:
          name: xref_b
          fail query: |
            SELECT sgt.dl_id FROM silver_g sgt JOIN o ON o.id = sgt.id
            WHERE ${INCREMENTAL_CONDITIONS:sgt}
    """.strip()

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(sodacl),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    spec    = yaml.safe_load(new_sodacl)
    query_a = spec["checks for silver_t"][0]["failed rows"]["fail query"]
    query_b = spec["checks for silver_t"][1]["failed rows"]["fail query"]

    assert "spo.dl_event_tms >" in query_a
    assert "sgt." not in query_a
    assert "sgt.dl_event_tms >" in query_b
    assert "spo." not in query_b
    assert set(per_check_wm.keys()) == {"xref_a", "xref_b"}


def test_mixed_bare_e_aliased_nello_stesso_campo():
    """Un campo con DUE placeholder (uno nudo, uno aliased) li risolve entrambi
    in un solo passaggio, ciascuno secondo il proprio alias."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    sodacl = """
    checks for silver_t:
      - failed rows:
          name: xref_mixed
          fail query: |
            SELECT spo.dl_id FROM silver_t spo
            WHERE col_x IS NULL AND ${INCREMENTAL_CONDITIONS}
              AND spo.id IN (
                SELECT id FROM silver_t spo WHERE ${INCREMENTAL_CONDITIONS:spo}
              )
    """.strip()

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(sodacl),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    query = yaml.safe_load(new_sodacl)["checks for silver_t"][0]["failed rows"]["fail query"]

    assert "${INCREMENTAL_CONDITIONS" not in query
    assert " dl_event_tms >" in query        # forma nuda (preceduta da spazio)
    assert "spo.dl_event_tms >" in query     # forma qualificata
    assert "xref_mixed" in per_check_wm


def test_filter_nativo_aliased_preserva_la_condizione_business():
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    sodacl = """
    checks for silver_t:
      - missing_count(after.iuv) = 0:
          name: fld__cmp__iuv__not_null
          filter: op IN ('c', 'r', 'u') AND ${INCREMENTAL_CONDITIONS:spo}
    """.strip()

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(sodacl),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    flt = yaml.safe_load(new_sodacl)["checks for silver_t"][0][
        "missing_count(after.iuv) = 0"
    ]["filter"]

    assert "op IN ('c', 'r', 'u')" in flt
    assert "AND spo.dl_event_tms >" in flt
    assert "${INCREMENTAL_CONDITIONS" not in flt


def test_contract_solo_aliased_e_rilevato_come_incrementale():
    """Prova che _incremental_fields usa la regex e non un match a sottostringa:
    un contract con SOLO placeholder aliased deve comunque essere processato."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)

    with patch.object(engine, "_lookup_check_watermark", return_value=None):
        _, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(SODACL_ALIASED),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    assert "xref__cns__ref_integrity" in per_check_wm


def test_alias_malformato_non_matcha_e_resta_nel_sodacl():
    """Un alias malformato (es. ':a.b' con punto) NON matcha affatto: il token
    resta verbatim e il check NON e' considerato incrementale (fail-fast a valle
    sullo scan, mai sostituzione silenziosa/parziale)."""
    cfg     = _make_config()
    scan_ts = datetime(2026, 5, 27, 3, 0, 0)
    sodacl = """
    checks for silver_t:
      - failed rows:
          name: xref_malformato
          fail query: |
            SELECT spo.dl_id FROM silver_t spo
            WHERE ${INCREMENTAL_CONDITIONS:a.b}
    """.strip()

    with patch.object(engine, "_lookup_check_watermark") as m:
        new_sodacl, per_check_wm = engine._resolve_per_check_watermarks(
            spark=None,
            config=cfg,
            contract=_make_contract(sodacl),
            scan_ts=scan_ts,
            watermark_column="dl_event_tms",
            cli_override=None,
        )

    m.assert_not_called()
    assert per_check_wm == {}
    assert "${INCREMENTAL_CONDITIONS:a.b}" in new_sodacl


def test_guard_regex_rileva_placeholder_solo_aliased():
    """Regressione del 'trap' della guard: la sottostringa nuda NON e' contenuta
    in un placeholder aliased, quindi il vecchio check `in` fallirebbe; la regex
    invece lo rileva correttamente."""
    sodacl_solo_aliased = (
        "checks for t:\n  - failed rows:\n      name: x\n"
        "      fail query: SELECT 1 WHERE ${INCREMENTAL_CONDITIONS:spo}\n"
    )

    # Il vecchio comportamento (substring) NON rileverebbe il placeholder...
    assert "${INCREMENTAL_CONDITIONS}" not in sodacl_solo_aliased
    # ...mentre la regex usata da guard e detection lo rileva.
    assert engine._INCREMENTAL_RE.search(sodacl_solo_aliased) is not None
