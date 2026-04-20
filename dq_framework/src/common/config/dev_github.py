from .base import AppConfig

DEV_GITHUB_CONFIG = AppConfig(
    env="dev-github",
    github_api_base_url="https://api.github.com",
    contract_path="https://github.com/carlomanco-qty/qty-data-contracts/blob/main/src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml",
    soda_host="cloud.soda.io",
    data_source="lg_spark",
    table_limit=50,
    ignore_datacontract_cli=False,
    soda_fallback_path="",
)
