"""Test della funzione _build_incremental_conditions.

Verifica la stringa SQL generata dal builder:
    - formato TIMESTAMP literal corretto
    - estremi (esclusivo a sinistra, inclusivo a destra)
    - microsecondi preservati
    - quotaggio coerente
"""
from __future__ import annotations

from datetime import datetime

from dq_framework.quality.engine import _build_incremental_conditions


def test_genera_clausola_base_con_microsecondi():
    wm_from = datetime(2026, 5, 24, 3, 0, 0, 123456)
    wm_to   = datetime(2026, 5, 27, 3, 0, 0, 654321)

    result = _build_incremental_conditions("dl_event_tms", wm_from, wm_to)

    assert result == (
        "dl_event_tms > TIMESTAMP '2026-05-24 03:00:00.123456' "
        "AND dl_event_tms <= TIMESTAMP '2026-05-27 03:00:00.654321'"
    )


def test_estremi_corretti_sinistro_escluso_destro_incluso():
    """L'intervallo deve essere (wm_from, wm_to]."""
    cond = _build_incremental_conditions(
        "ts", datetime(2026, 1, 1), datetime(2026, 1, 2)
    )
    assert " > TIMESTAMP " in cond
    assert " <= TIMESTAMP " in cond
    assert " >= TIMESTAMP " not in cond
    assert " < TIMESTAMP "  not in cond


def test_microsecondi_a_zero_vengono_mantenuti():
    """strftime('%f') deve produrre sempre 6 cifre, anche con microsecondi=0."""
    wm_from = datetime(2026, 5, 24, 0, 0, 0)
    wm_to   = datetime(2026, 5, 25, 0, 0, 0)

    cond = _build_incremental_conditions("dl_event_tms", wm_from, wm_to)

    assert "TIMESTAMP '2026-05-24 00:00:00.000000'" in cond
    assert "TIMESTAMP '2026-05-25 00:00:00.000000'" in cond


def test_supporta_colonne_qualificate():
    """La colonna passata viene inserita verbatim — supporta anche nomi annidati."""
    cond = _build_incremental_conditions(
        "events.updated_at", datetime(2026, 5, 24), datetime(2026, 5, 25)
    )
    assert cond.startswith("events.updated_at > TIMESTAMP")
    assert "AND events.updated_at <= TIMESTAMP" in cond


def test_alias_qualifica_la_colonna():
    """Con alias la colonna viene prefissata con <alias>. (caso JOIN)."""
    cond = _build_incremental_conditions(
        "dl_event_tms", datetime(2026, 5, 24), datetime(2026, 5, 25), alias="spo"
    )
    assert cond.startswith("spo.dl_event_tms > TIMESTAMP")
    assert "AND spo.dl_event_tms <= TIMESTAMP" in cond


def test_alias_none_e_byte_identico_alla_forma_nuda():
    """alias=None deve produrre output identico alla chiamata senza alias
    (garanzia di retrocompatibilita' totale)."""
    wm_from = datetime(2026, 5, 24, 3, 0, 0, 123456)
    wm_to   = datetime(2026, 5, 27, 3, 0, 0, 654321)

    nudo     = _build_incremental_conditions("dl_event_tms", wm_from, wm_to)
    none_arg = _build_incremental_conditions(
        "dl_event_tms", wm_from, wm_to, alias=None
    )
    assert nudo == none_arg
