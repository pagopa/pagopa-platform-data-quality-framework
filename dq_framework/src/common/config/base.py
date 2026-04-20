from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    env: str

    # GitHub
    github_api_base_url: str
    contract_path: str          # path locale o URL github.com/blob/…

    # Soda
    soda_host: str
    data_source: str
    table_limit: int

    # Comportamento DataContract CLI
    ignore_datacontract_cli: bool
    soda_fallback_path: str     # usato solo quando ignore_datacontract_cli=True
