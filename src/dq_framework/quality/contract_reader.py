"""Lettura del Data Contract: solo I/O.

Risolve il path (locale o GitHub), scarica via Contents API se remoto e legge
lo YAML in un dict grezzo. Nessuna trasformazione, nessuna conoscenza del
SodaCL: quella vive in `contract_parser`.
"""

from __future__ import annotations

import base64
import logging
import tempfile

import requests
import yaml

from dq_framework.common.config import AppConfig
from dq_framework.common import secrets

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


def read_contract_doc(
    contract_path: str,
    repository: str,
    ref: str,
    config: AppConfig,
) -> tuple[str, dict | None]:
    """Risolve il path e legge lo YAML del Data Contract in un dict grezzo.

    Ritorna `(filepath, doc)`; `doc` è None se path-resolution o lettura
    falliscono (l'errore è loggato). Il `filepath` ritornato è il path locale
    effettivo (per i contract remoti è il file temporaneo scaricato).
    """
    try:
        filepath = _resolve_contract_path(contract_path, repository, ref, config)
    except RuntimeError as e:
        logger.error(str(e))
        return contract_path, None

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        logger.info(f"Lettura YAML da {filepath} riuscita con successo!")
        return filepath, doc
    except Exception as e:
        logger.error(f"Errore durante la lettura del file {filepath}: {e}")
        return filepath, None
