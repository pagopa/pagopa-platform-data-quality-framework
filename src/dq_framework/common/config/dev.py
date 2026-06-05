from .base import AppConfig

DEV_CONFIG = AppConfig(
    env="dev",
    github_api_base_url="https://api.github.com",
    contract_path="./tests/fixtures/contracts/payment-position claude v2.yaml",
    default_repository="",          # vuoto = tratta contract_path come path locale
    default_ref="main",
    soda_host="cloud.soda.io",
    data_source="pagopa_qa",
    table_limit=50,
    results_database="pagopa_dev",
    results_write_enabled=False,
    results_table_location=None,
)
