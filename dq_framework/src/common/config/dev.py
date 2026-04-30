from .base import AppConfig

DEV_CONFIG = AppConfig(
    env="dev",
    github_api_base_url="https://api.github.com",
    contract_path="./dq_framework/tests/fixtures/contracts/dc payment-option v7.yaml",
    default_repository="",          # vuoto = tratta contract_path come path locale
    default_ref="main",
    soda_host="cloud.soda.io",
    data_source="pagopa_qa",
    table_limit=50,
)
