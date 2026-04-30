from __future__ import annotations

import base64
import logging
import os
import re
import sys
import tempfile

import requests
import yaml

from dq_framework.src.common.config import AppConfig
from dq_framework.src.common import secrets

logger = logging.getLogger(__name__)


_GITHUB_API_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def _fetch_from_github(
    repository: str,
    ref: str,
    filepath: str,
    config: AppConfig,
) -> str:
    """Scarica un file dal repo GitHub via Contents API e lo salva in un file temp. Ritorna il path temp."""
    try:
        owner, repo = repository.split("/", 1)
    except ValueError as exc:
        raise RuntimeError(
            f"Repository '{repository}' non valido: atteso formato 'owner/repo'."
        ) from exc

    api_url = f"{config.github_api_base_url}/repos/{owner}/{repo}/contents/{filepath}"

    token = secrets.github_token()
    headers = dict(_GITHUB_API_HEADERS)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        logger.warning("GITHUB_TOKEN non impostato: il download fallirà su repo privati.")

    logger.info(f"Download Data Contract via GitHub API: {api_url} (ref={ref})")
    response = requests.get(api_url, headers=headers, params={"ref": ref}, timeout=10)

    if response.status_code == 401:
        raise RuntimeError("Download fallito: GITHUB_TOKEN mancante o non valido (401 Unauthorized).")
    if response.status_code == 403:
        raise RuntimeError(
            "Download fallito: accesso negato (403 Forbidden). "
            "Verificare che GITHUB_TOKEN abbia i permessi 'Contents: Read' sul repo."
        )
    if response.status_code == 404:
        raise RuntimeError(f"Download fallito: file non trovato (404). Verificare URL e permessi: {api_url}")
    response.raise_for_status()

    payload = response.json()
    if payload.get("encoding") != "base64":
        raise RuntimeError(f"Encoding inatteso dalla GitHub API: {payload.get('encoding')!r}")

    content = base64.b64decode(payload["content"]).decode("utf-8")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml", mode="w", encoding="utf-8")
    tmp.write(content)
    tmp.close()
    logger.info(f"Contract scaricato in file temporaneo: {tmp.name}")
    return tmp.name


def _resolve_contract_path(
    contract_path: str,
    repository: str,
    ref: str,
    config: AppConfig,
) -> str:
    """Restituisce un path locale leggibile.

    - Se `repository` è vuoto → `contract_path` è un path locale: viene restituito così com'è.
    - Altrimenti → `contract_path` è repo-relativo e viene scaricato via GitHub API.
    """
    if not repository:
        return contract_path
    return _fetch_from_github(repository, ref, contract_path, config)





def _normalize_sodacl(sodacl: str, dataset: str, table_name: str) -> str:
    """Assicura che il nome tabella in SodaCL corrisponda al nome della vista temporanea Spark."""
    normalized = re.sub(
        r"checks for [^\s:]+:",
        f"checks for {table_name}:",
        sodacl,
    )
    return normalized.replace(dataset, table_name)


def parse_contract_file(
    contract_path: str,
    repository: str,
    ref: str,
    config: AppConfig,
) -> dict | None:
    """Legge il file YAML del Data Contract ed estrae la specifica SodaCL dal blocco 'quality'."""
    try:
        filepath = _resolve_contract_path(contract_path, repository, ref, config)
    except RuntimeError as e:
        logger.error(str(e))
        return None

    try:
        # Leggiamo il file come un normale dizionario YAML
        with open(filepath, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
            
        logger.info(f"Lettura YAML da {filepath} riuscita con successo!")
        
        # 1. Estrazione metadati base
        info = doc.get("info", {})
        contract_title = info.get("title", os.path.basename(filepath))
        contract_version = str(info.get("version", "1.0"))
        
        # 2. Ricerca del blocco "quality"
        quality_block = doc.get("quality", {})
        if not quality_block or quality_block.get("type") != "SodaCL":
            logger.error(f"File saltato '{filepath}': manca il blocco 'quality' di tipo 'SodaCL'.")
            return None
            
        # 3. Estrazione dataset e stringa SodaCL pura
        dataset = quality_block.get("dataset", "")
        raw_sodacl = quality_block.get("specification", "")
        
        if not dataset or not raw_sodacl:
            logger.error(f"File saltato '{filepath}': 'dataset' o 'specification' mancanti nel blocco 'quality'.")
            return None
            
        # Calcoliamo il nome della tabella in base al dataset
        table_name = dataset.split(".")[-1].replace("-", "_")

        # 4. Recupero eventuali check Impala custom
        impala_quality_checks = doc.get("custom_impala_quality", [])

        logger.debug(f"\n{'-'*30} CONTROLLI SODA ESTRATTI {'-'*30}\n{raw_sodacl}\n{'-'*88}")

    except Exception as e:
        logger.error(f"Errore durante l'elaborazione del file {filepath}: {str(e)}")
        return None

    return {
        "contract_path":    filepath,
        "contract_title":   contract_title,
        "contract_version": contract_version,
        "dataset":          dataset,
        "table_name":       table_name,
        "sodacl":           _normalize_sodacl(raw_sodacl, dataset, table_name),
        "impala_checks":    impala_quality_checks,
    }