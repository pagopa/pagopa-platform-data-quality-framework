from __future__ import annotations

import os

from .base import AppConfig
from .dev import DEV_CONFIG
from .dev_github import DEV_GITHUB_CONFIG
from .test import TEST_CONFIG
from .prod import PROD_CONFIG

_REGISTRY: dict[str, AppConfig] = {
    "dev": DEV_CONFIG,
    "dev-github": DEV_GITHUB_CONFIG,
    "test": TEST_CONFIG,
    "prod": PROD_CONFIG,
}


def load_config() -> AppConfig:
    """Restituisce l'AppConfig per l'ambiente corrente (ENV, default: dev)."""
    env = os.getenv("ENV", "dev").lower()
    cfg = _REGISTRY.get(env)
    if cfg is None:
        raise ValueError(
            f"ENV={env!r} non supportato. Valori validi: {list(_REGISTRY)}"
        )
    return cfg
