"""Parsing del Data Contract: trasformazioni pure (niente I/O, niente Spark).

L'I/O vive in `contract_reader`. Qui estraiamo metadati, dataset e SodaCL dal
documento YAML, normalizziamo i nomi tabella per Spark ed esponiamo
`extract_and_clean_failed_queries` per la gestione delle failed-query differite.
"""

from __future__ import annotations

import logging
import os
import re

import yaml

from dq_framework.common.config import AppConfig
from .contract_reader import read_contract_doc

logger = logging.getLogger(__name__)


def _normalize_sodacl(sodacl: str, dataset: str, table_name: str, xref_datasets: list[str]) -> str:
    """Allinea i nomi tabella del SodaCL alle temp view Spark.

    Tre trasformazioni:
      1. riscrive l'header ``checks for <x>:`` in ``checks for <table_name>:``;
      2. sostituisce ovunque la stringa ``dataset`` con ``table_name``;
      3. per ogni xref rimuove il prefisso DB (``db.tabella`` -> ``tabella``) e
         mappa al leaf normalizzato.

    NB: la sostituzione (2)/(3) è un replace testuale: se un leaf xref è
    sottostringa del nome tabella principale può sovra-sostituire. Comportamento
    storico preservato; tenerlo presente estendendo le regole.
    """
    # 1. Normalizzazione tabella principale
    normalized = re.sub(
        r"checks for [^\s:]+:",
        f"checks for {table_name}:",
        sodacl,
    )
    normalized = normalized.replace(dataset, table_name)

    # 2. Normalizzazione tabelle xref (rimozione prefisso DB)
    for xref in xref_datasets:
        xref_table_name = xref.split(".")[-1].replace("-", "_")
        # Rimuove qualsiasi prefisso (es. db.schema.tabella -> tabella)
        pattern = r'\b[a-zA-Z0-9_]+\.' + re.escape(xref_table_name) + r'\b'
        normalized = re.sub(pattern, xref_table_name, normalized)
        normalized = normalized.replace(xref, xref_table_name)

    return normalized


def extract_and_clean_failed_queries(sodacl_yaml: str) -> tuple[str, dict[str, dict]]:
    """Estrae i 'failed-query-fields' dal SodaCL e li rimuove dallo YAML.

    Soda non conosce la chiave 'failed-query-fields': la togliamo per non far
    fallire l'engine e salviamo (query massiva + campi) per l'esecuzione
    differita sulle righe fallite. Va invocata DOPO la sostituzione watermark,
    così la query salvata ha già i timestamp risolti.

    Ritorna `(cleaned_yaml, extracted)` dove `extracted` è
    `{check_name: {"query": ..., "fields": ...}}`.
    """
    spec_dict = yaml.safe_load(sodacl_yaml)
    extracted: dict[str, dict] = {}

    if not isinstance(spec_dict, dict):
        return sodacl_yaml, extracted

    for key in spec_dict:
        if not isinstance(key, str) or not key.startswith("checks for "):
            continue

        check_list = spec_dict[key]
        if not isinstance(check_list, list):
            continue

        for check_item in check_list:
            if not isinstance(check_item, dict):
                continue

            for check_type, check_body in check_item.items():
                if not isinstance(check_body, dict):
                    continue

                if "failed-query-fields" not in check_body:
                    continue

                fields = check_body.pop("failed-query-fields")
                check_name = check_body.get("name")
                # Chiave della query massiva (es. 'mio_check query')
                query_key = next(
                    (k for k in check_body if isinstance(k, str) and k.endswith(" query")),
                    None,
                )

                if not (check_name and query_key and isinstance(fields_raw, str) and fields_raw.strip()):
                    logger.warning(
                        f"'failed-query-fields' ignorato: name/query/fields mancanti o "
                        f"vuoti (check='{check_name}', query_key={query_key!r})."
                    )
                    continue

                extracted[check_name] = {"query": check_body[query_key], "fields": fields_raw}

    cleaned_yaml = yaml.safe_dump(spec_dict, sort_keys=False, allow_unicode=True)
    return cleaned_yaml, extracted


def parse_contract_file(
    contract_path: str,
    repository: str,
    ref: str,
    config: AppConfig,
    xref_datasets_override: list[str] | None = None,
) -> dict | None:
    """Legge il Data Contract ed estrae la specifica SodaCL dal blocco 'quality'."""
    filepath, doc = read_contract_doc(contract_path, repository, ref, config)
    if doc is None:
        return None

    try:
        info = doc.get("info", {})
        contract_title = info.get("title", os.path.basename(filepath))
        contract_version = str(info.get("version", "1.0"))

        quality_block = doc.get("quality", {})
        if not quality_block or quality_block.get("type") != "SodaCL":
            logger.error(f"File saltato '{filepath}': manca il blocco 'quality' di tipo 'SodaCL'.")
            return None

        dataset = quality_block.get("dataset", "")
        raw_sodacl = quality_block.get("specification", "")

        # Lettura originale dal Data Contract
        xref_datasets_raw = quality_block.get("xref-dataset", [])
        xref_datasets = xref_datasets_raw if isinstance(xref_datasets_raw, list) else [xref_datasets_raw]

        # SE IL DAG HA PASSATO UN OVERRIDE, APPLICALO CON PRIORITÀ
        if xref_datasets_override is not None:
            logger.info(f"Applicato override xref-dataset da DAG: {xref_datasets_override}")
            xref_datasets = xref_datasets_override

        if not dataset or not raw_sodacl:
            logger.error(f"File saltato '{filepath}': 'dataset' o 'specification' mancanti nel blocco 'quality'.")
            return None

        # table_name = leaf del dataset con '-' -> '_': diventa il nome di una temp
        # view Spark usata come identificatore SQL non quotato, che non ammette trattini.
        table_name = dataset.split(".")[-1].replace("-", "_")

        normalized_sodacl = _normalize_sodacl(raw_sodacl, dataset, table_name, xref_datasets)

        logger.debug(f"\n{'-'*30} CONTROLLI SODA ESTRATTI {'-'*30}\n{normalized_sodacl}\n{'-'*88}")

    except Exception as e:
        logger.error(f"Errore durante l'elaborazione del file {filepath}: {str(e)}")
        return None

    return {
        "contract_path":    filepath,
        "contract_title":   contract_title,
        "contract_version": contract_version,
        "dataset":          dataset,
        "table_name":       table_name,
        "xref_datasets":    xref_datasets,
        "sodacl":           normalized_sodacl,
    }
