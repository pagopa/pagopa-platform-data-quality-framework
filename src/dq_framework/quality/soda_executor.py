from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from soda.scan import Scan
from soda.sampler.sampler import Sampler
from soda.sampler.sample_ref import SampleRef

from dq_framework.common.config import AppConfig
from dq_framework.common import secrets

logger = logging.getLogger(__name__)


class MemorySampler(Sampler):
    """Custom Sampler iper-difensivo per estrarre failed rows da Soda in RAM senza errori."""
    def __init__(self):
        self.failed_data = {}

    def store_sample(self, *args, **kwargs) -> SampleRef:
        if "sample_context" in kwargs:
            sample_context = kwargs["sample_context"]
        elif len(args) == 2:
            sample_context = args[1]
        else:
            sample_context = args[0]

        check_name = sample_context.check_name
        rows = []
        schema = None

        try:
            sample = sample_context.sample
            if sample:
                rows = sample.get_rows()
                schema = sample.get_schema()
                cols = []

                if hasattr(schema, 'columns'):
                    cols = [c.name if hasattr(c, 'name') else c.get('name') for c in schema.columns]
                elif hasattr(schema, 'get_dict'):
                    # Fallback al dizionario serializzato
                    d = schema.get_dict()
                    if 'columns' in d:
                        cols = [c.get('name') for c in d['columns']]

                if not cols and rows:
                    cols = [f"col_{i}" for i in range(len(rows[0]))]

                if rows:
                    list_of_dicts = [dict(zip(cols, r)) for r in rows]
                    self.failed_data[check_name] = list_of_dicts
                
        except Exception as e:
            logger.warning(f"Impossibile estrarre le righe in RAM per il check '{check_name}': {e}")

        row_count = len(rows) if rows else 0
        sample_name = getattr(sample_context, 'sample_name', 'failed_rows') or "failed_rows"
        
        return SampleRef(
            name=sample_name,
            schema=schema,
            total_row_count=row_count,
            stored_row_count=row_count,
            type="failed_rows"
        )


def run_dataframe_soda_scan(spark: SparkSession, contract: dict, config: AppConfig) -> tuple[list[dict], int, MemorySampler]:
    """Esegue la scansione SodaCL e restituisce: check, totale righe e il sampler contenente i failed_rows."""
    
    limit_msg = f"(LIMIT {config.table_limit})" if config.table_limit and config.table_limit > 0 else "(NESSUN LIMITE)"
    logger.info(f"Caricamento dataframe per dataset: {contract['dataset']} {limit_msg}")

    # Estraiamo il DB principale come fallback per le xref
    main_db = contract["dataset"].split(".")[0] if "." in contract["dataset"] else ""

    try:
        # 1. Caricamento Dataset Principale
        safe_dataset = ".".join([f"`{part}`" for part in contract["dataset"].split(".")])
        df = spark.table(safe_dataset)
        
        if config.table_limit and config.table_limit > 0:
            df = df.limit(config.table_limit)

        total_rows = df.count()
        logger.info(f"Il DataFrame contiene {total_rows} righe che verranno scansionate.")
            
        df.createOrReplaceTempView(contract["table_name"])
        
        # 2. Caricamento Dataset XREF (Cross-Reference)
        for xref in contract.get("xref_datasets", []):
            if not xref:
                continue
            
            xref_full_name = f"{main_db}.{xref}" if "." not in xref and main_db else xref
            xref_safe = ".".join([f"`{part}`" for part in xref_full_name.split(".")])
            
            xref_table_name = xref.split(".")[-1].replace("-", "_")
            
            try:
                df_xref = spark.table(xref_safe)
                
                df_xref.createOrReplaceTempView(xref_table_name)
                logger.info(f"Temp View XREF creata con successo: {xref_table_name} (da {xref_full_name})")
            except Exception as e:
                logger.error(f"Errore durante la creazione della Temp View XREF per '{xref_full_name}': {e}")
                
    except Exception as e:
        logger.error(f"Errore caricamento tabella Spark {contract['dataset']}: {e}")
        return [], 0, MemorySampler()

    logger.info(f"Esecuzione Soda Scan per la vista temporanea '{contract['table_name']}'...")
    logger.info(f"\n{'-'*30} Controlli generati {'-'*30}\n{contract['sodacl']}\n{'-'*88}")
    
    scan = Scan()
    
    # ---- INIEZIONE DEL SAMPLER IN MEMORIA ----
    sampler = MemorySampler()
    scan.sampler = sampler
    # ------------------------------------------

    scan.set_data_source_name(config.data_source)
    scan.add_spark_session(spark, data_source_name=config.data_source)
    scan.add_sodacl_yaml_str(contract["sodacl"])

    soda_api_key    = secrets.soda_api_key()
    soda_api_secret = secrets.soda_api_secret()

    if soda_api_key and soda_api_secret:
        logger.info("Credenziali Soda Cloud rilevate. Invio metriche aggregate attivato (nessun dettaglio record).")
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
    checks = scan.get_scan_results().get("checks", [])

    return checks, total_rows, sampler