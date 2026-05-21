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

    # Results sink (Iceberg)
    results_database: str               # es. "pagopa_dev" / "pagopa_prod"
    results_write_enabled: bool = False # se False la pipeline non scrive su DB (utile in locale)
    results_table: str = "dqf_gpd_results"
    results_table_location: str | None = None  # opzionale: LOCATION usata in CREATE TABLE IF NOT EXISTS