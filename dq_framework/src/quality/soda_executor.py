from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from soda.scan import Scan

from dq_framework.src.common.config import AppConfig
from dq_framework.src.common import secrets

logger = logging.getLogger(__name__)


def run_soda_scan(spark: SparkSession, contract: dict, config: AppConfig) -> list[dict]:
    """Esegue la scansione SodaCL sulla tabella Spark e restituisce i check grezzi."""
    logger.info(f"Caricamento dataframe per dataset: {contract['dataset']} (LIMIT {config.table_limit})")

    try:
        safe_dataset = ".".join([f"`{part}`" for part in contract["dataset"].split(".")])
        df = spark.table(safe_dataset).limit(config.table_limit)
        df.createOrReplaceTempView(contract["table_name"])
    except Exception as e:
        logger.error(f"Errore caricamento tabella Spark {contract['dataset']}: {e}")
        return []

    logger.info(f"Esecuzione Soda Scan per la vista temporanea '{contract['table_name']}'...")
    scan = Scan()
    scan.set_data_source_name(config.data_source)
    scan.add_spark_session(spark, data_source_name=config.data_source)
    scan.add_sodacl_yaml_str(contract["sodacl"])

    soda_api_key    = secrets.soda_api_key()
    soda_api_secret = secrets.soda_api_secret()

    if soda_api_key and soda_api_secret:
        logger.info("Credenziali Soda Cloud rilevate: invio risultati in corso...")
        soda_cfg = f"""
        soda_cloud:
          host: {config.soda_host}
          api_key_id: {soda_api_key}
          api_key_secret: {soda_api_secret}
          samples_limit: 100
        """
        scan.add_configuration_yaml_str(soda_cfg)
        scan.set_scan_definition_name(contract["contract_title"])
    else:
        logger.warning("Credenziali Soda Cloud mancanti: l'esecuzione avverrà solo in locale.")

    scan.execute()
    return scan.get_scan_results().get("checks", [])
