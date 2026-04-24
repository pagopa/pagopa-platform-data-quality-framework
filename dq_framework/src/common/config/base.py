from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    env: str

    # GitHub
    github_api_base_url: str
    contract_path: str           # default: path repo-relativo (dev-github/prod) o path locale (dev)
    default_repository: str      # "owner/repo" — vuoto se contract_path è locale
    default_ref: str             # branch/tag/commit di default

    # Soda
    soda_host: str
    data_source: str
    table_limit: int

    # Comportamento DataContract CLI
    ignore_datacontract_cli: bool
    soda_fallback_path: str      # usato solo quando ignore_datacontract_cli=True
