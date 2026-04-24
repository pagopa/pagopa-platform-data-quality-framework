from .base import AppConfig

PROD_CONFIG = AppConfig(
    env="prod",
    github_api_base_url="https://api.github.com",
    contract_path="src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml",
    default_repository="carlomanco-qty/qty-data-contracts",
    default_ref="main",
    soda_host="cloud.soda.io",
    data_source="Raptor Lake",
    table_limit=200,
    ignore_datacontract_cli=False,
    soda_fallback_path="",
)
