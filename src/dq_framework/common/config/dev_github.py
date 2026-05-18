from .base import AppConfig

DEV_GITHUB_CONFIG = AppConfig(
    env="dev-github",
    github_api_base_url="https://api.github.com",
    contract_path="src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml",
    default_repository="carlomanco-qty/qty-data-contracts",
    default_ref="main",
    soda_host="cloud.soda.io",
    data_source="lg_spark",
    table_limit=0,
)
