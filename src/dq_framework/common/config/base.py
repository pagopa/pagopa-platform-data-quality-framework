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
    
    results_table_location: str | None = None  # opzionale: LOCATION usata in CREATE TABLE IF NOT EXISTS

    # Controlli incrementali
    #   Placeholder atteso nelle query SodaCL custom; quando presente in una query
    #   il framework risolve il watermark per-check e sostituisce monoliticamente
    #   con "<col> > TIMESTAMP '...' AND <col> <= TIMESTAMP '...'".
    incremental_placeholder: str = "${INCREMENTAL_CONDITIONS}"
    #   Colonna di default usata per espandere il placeholder. Override possibile
    #   via CLI flag --watermark-column. Se None ed esiste il placeholder nel
    #   contract: errore fail-fast.
    default_watermark_column: str | None = "dl_event_tms"
    #   Minuti di lookback applicati a wm_from per coprire late arrivals.
    #   Solo per lookup automatico da Iceberg; non si applica a CLI/Airflow override.
    incremental_lookback_minutes: int = 0

    #   Numero massimo di record falliti campionati per check e scritti nella
    #   tabella failed_records. Vale per entrambi i path di cattura: i sample dei
    #   check nativi Soda (MemorySampler) e il LIMIT delle failed-query custom
    #   ricostruite a posteriori.
    failed_sample_limit: int = 5
    #   Primary key surrogate usate per failed_records quando il DAG non passa
    #   --primary-keys. Tuple perche' la dataclass e' frozen (default immutabile).
    default_primary_keys: tuple[str, ...] = ("dl_id",)