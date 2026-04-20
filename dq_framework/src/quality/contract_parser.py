from __future__ import annotations

import base64
import logging
import os
import re
import sys
import tempfile

import requests
import yaml

try:
    from datacontract.data_contract import DataContract
except ImportError:
    print("ERRORE CRITICO: Libreria 'datacontract' non trovata. Verificare il Virtual Environment CDE.")
    sys.exit(1)

from dq_framework.src.common.config import AppConfig

logger = logging.getLogger(__name__)


_GITHUB_BLOB_RE = re.compile(r"https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)")

_GITHUB_API_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def _parse_github_url(url: str) -> tuple[str, str, str, str] | None:
    """Estrae (owner, repo, ref, filepath) da una URL github.com/blob/."""
    match = _GITHUB_BLOB_RE.match(url)
    if match:
        return match.groups()  # (owner, repo, ref, filepath)
    return None


def _resolve_contract_path(path: str, config: AppConfig) -> str:
    """Restituisce un path locale: scarica il file via GitHub API se è una URL GitHub, altrimenti lo usa direttamente."""
    if not path.startswith("https://"):
        return path

    parts = _parse_github_url(path)
    if parts is None:
        raise RuntimeError(
            f"URL GitHub non riconosciuta (formato atteso: github.com/{{owner}}/{{repo}}/blob/{{ref}}/{{path}}): {path}"
        )

    owner, repo, ref, filepath = parts
    api_url = f"{config.github_api_base_url}/repos/{owner}/{repo}/contents/{filepath}"

    token = os.getenv("GITHUB_TOKEN")
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


def _generate_sodacl(filepath: str) -> str | None:
    """Converte un Data Contract YAML in stringa SodaCL tramite l'API Python nativa."""
    logger.info(f"Avvio conversione DataContract -> SodaCL per: {filepath}")
    try:
        data_contract = DataContract(data_contract_file=filepath)
        sodacl_string = data_contract.export(export_format="sodacl")
        logger.info("Conversione riuscita con successo!")
        logger.info(f"\n{'-'*30} CONTROLLI GENERATI (DEBUG) {'-'*30}\n{sodacl_string}\n{'-'*88}")
        return sodacl_string
    except Exception as e:
        logger.error(f"Errore durante la conversione tramite DataContract API: {str(e)}")
        return None


def _normalize_sodacl(sodacl: str, dataset: str, table_name: str) -> str:
    """Assicura che il nome tabella in SodaCL corrisponda al nome della vista temporanea Spark."""
    normalized = re.sub(
        r"checks for [^\s:]+:",
        f"checks for {table_name}:",
        sodacl,
    )
    return normalized.replace(dataset, table_name)


def parse_contract_file(path: str, config: AppConfig) -> dict | None:
    """Legge metadati essenziali dallo YAML e genera la stringa SodaCL corrispondente.

    Accetta sia un path locale che una URL github.com/blob/ (scaricata via GitHub API).
    """
    try:
        filepath = _resolve_contract_path(path, config)
    except RuntimeError as e:
        logger.error(str(e))
        return None

    try:
        with open(filepath, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception as e:
        logger.error(f"Impossibile leggere il file YAML {filepath}: {e}")
        return None

    if "dataContractSpecification" not in doc or "models" not in doc:
        logger.error(f"File saltato '{filepath}': manca 'dataContractSpecification' o 'models'.")
        return None

    models = doc.get("models", {})
    if not models:
        logger.error(f"File saltato '{filepath}': il tag 'models' è presente ma vuoto.")
        return None

    model_name = list(models.keys())[0].strip()
    dataset = models[model_name].get("dataset", model_name).strip()

    info = doc.get("info", {})
    table_name = dataset.split(".")[-1].replace("-", "_")

    raw_sodacl: str | None = None

    if config.ignore_datacontract_cli:
        logger.info(f"ignore_datacontract_cli=True: lettura diretta da {config.soda_fallback_path}")
        try:
            with open(config.soda_fallback_path, "r", encoding="utf-8") as f:
                raw_sodacl = f.read()
            logger.info("Lettura da soda_fallback_path riuscita con successo!")
            logger.info(f"\n{'-'*30} CONTROLLI CARICATI (DEBUG) {'-'*30}\n{raw_sodacl}\n{'-'*88}")
        except Exception as e:
            logger.error(f"Errore durante la lettura del file {config.soda_fallback_path}: {str(e)}")
    else:
        raw_sodacl = _generate_sodacl(filepath)

    if not raw_sodacl:
        logger.warning(f"File saltato '{filepath}': generazione SodaCL fallita.")
        return None

    return {
        "contract_path":    filepath,
        "contract_title":   info.get("title", os.path.basename(filepath)),
        "contract_version": str(info.get("version", "")),
        "dataset":          dataset,
        "table_name":       table_name,
        "sodacl":           _normalize_sodacl(raw_sodacl, dataset, table_name),
    }
