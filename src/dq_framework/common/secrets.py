"""Lettura segreti con pattern file-then-env.

Su CDE i secret sono montati come file in /etc/dex/secrets/<credential-name>/<chiave>
(quando il job è creato con `cde credential` di tipo workload-secret).
In locale i secret vengono dal .env tramite `include .env; export` del Makefile.

La funzione prova prima il file; se non esiste cade sulla variabile d'ambiente.
Questo evita di duplicare la logica e permette al codice di girare identico
nei due ambienti.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_CDE_SECRETS_ROOT = Path("/etc/dex/secrets")


def read_secret(cred_name: str, key: str, env_fallback: str) -> str | None:
    """Restituisce il valore del secret, o None se non disponibile.

    Ordine di lookup:
      1. /etc/dex/secrets/<cred_name>/<key>   (CDE)
      2. os.environ[<env_fallback>]           (locale, .env)
    """
    secret_file = _CDE_SECRETS_ROOT / cred_name / key
    if secret_file.exists():
        logger.debug(f"Secret '{cred_name}/{key}' letto da {secret_file}")
        return secret_file.read_text(encoding="utf-8").strip()

    value = os.getenv(env_fallback)
    if value:
        logger.debug(f"Secret '{cred_name}/{key}' letto da env var {env_fallback}")
        return value

    return None


# --- Accessor di comodo per i secret usati dall'applicazione --------------

def github_token() -> str | None:
    return read_secret("github-token", "token", "GITHUB_TOKEN")


def soda_api_key() -> str | None:
    return read_secret("soda-creds", "api_key", "SODA_API_KEY")


def soda_api_secret() -> str | None:
    return read_secret("soda-creds", "api_secret", "SODA_API_SECRET")
