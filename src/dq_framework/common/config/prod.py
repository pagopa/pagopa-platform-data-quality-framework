from .base import AppConfig

PROD_CONFIG = AppConfig(
    env="prod",
    github_api_base_url="https://api.github.com",
    contract_path="src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml",
    default_repository="carlomanco-qty/qty-data-contracts",
    default_ref="main",
    soda_host="cloud.soda.io",
    data_source="pagopa_qa",
    table_limit=0,
    results_database="pagopa_qa",
    results_write_enabled=True,
    # se la tabella esiste già in metastore, lasciare None: la CREATE TABLE IF NOT EXISTS sarà
    # comunque no-op. Valorizzare solo se si vuole che il primo run la crei su un path S3 specifico,
    # es. "s3a://pdnd-prod-dl-1/warehouse/tablespace/external/hive/pagopa_qa.db/silver_dqf_gpd_results"
    results_table_location=None,
)
