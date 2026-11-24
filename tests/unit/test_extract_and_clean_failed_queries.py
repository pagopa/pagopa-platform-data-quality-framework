"""Test di extract_and_clean_failed_queries (estrazione delle failed-query differite).

Verifica:
    - la chiave 'failed-query-fields', che Soda non conosce, viene rimossa dallo
      YAML passato all'engine
    - query massiva e campi vengono salvati per l'esecuzione differita
    - i check senza failed-query-fields non compaiono in `extracted`
    - fields vuoti/non stringa vengono ignorati senza far esplodere il parsing
"""
from __future__ import annotations

import yaml

from dq_framework.quality.contract_parser import extract_and_clean_failed_queries

SODACL = """
checks for silver_t:

  - fld__vld__max_due_date__timestamp_length = 0:
      fld__vld__max_due_date__timestamp_length query: |
        SELECT COUNT(*)
        FROM pagopa.silver_t
        WHERE LENGTH(CAST(after.max_due_date AS STRING)) > 16
      failed-query-fields: dl_id, after.iupd, after.max_due_date
      name: fld__vld__max_due_date__timestamp_length

  - massive_check = 0:
      massive_check query: |
        SELECT COUNT(*) FROM pagopa.silver_t WHERE col_x < 0
      name: check_massivo
""".strip()


def test_estrae_query_e_campi_e_rimuove_la_chiave_dallo_yaml():
    cleaned, extracted = extract_and_clean_failed_queries(SODACL)

    # Se la chiave restasse, l'engine Soda fallirebbe sullo schema del check
    assert "failed-query-fields" not in cleaned

    info = extracted["fld__vld__max_due_date__timestamp_length"]
    assert info["fields"] == "dl_id, after.iupd, after.max_due_date"
    assert "SELECT COUNT(*)" in info["query"]

    # Il check massivo non ha failed-query-fields: non va estratto
    assert "check_massivo" not in extracted


def test_lo_yaml_pulito_conserva_query_e_name_del_check():
    cleaned, _ = extract_and_clean_failed_queries(SODACL)
    spec = yaml.safe_load(cleaned)

    check = spec["checks for silver_t"][0]["fld__vld__max_due_date__timestamp_length = 0"]
    assert check["name"] == "fld__vld__max_due_date__timestamp_length"
    assert [k for k in check if k.endswith(" query")], "la chiave della query deve restare nello YAML"


def test_fields_vuoti_non_estraggono_nulla_e_non_sollevano():
    sodacl = SODACL.replace(
        "failed-query-fields: dl_id, after.iupd, after.max_due_date",
        'failed-query-fields: "   "',
    )

    cleaned, extracted = extract_and_clean_failed_queries(sodacl)

    assert extracted == {}
    assert "failed-query-fields" not in cleaned


def test_sodacl_non_dict_ritorna_input_invariato():
    cleaned, extracted = extract_and_clean_failed_queries("- solo una lista")

    assert cleaned == "- solo una lista"
    assert extracted == {}
