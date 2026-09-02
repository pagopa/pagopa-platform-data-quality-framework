from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel
from soda.scan import Scan
from soda.sampler.sampler import Sampler
from soda.sampler.sample_ref import SampleRef

from dq_framework.common.config import AppConfig
from dq_framework.common import secrets
from .errors import ScanExecutionError

logger = logging.getLogger(__name__)


class MemorySampler(Sampler):
    """Sampler iper-difensivo che estrae le failed rows da Soda in RAM senza errori.

    Cattura in locale (nessun I/O di rete): è ortogonale al `samples_limit` del
    blocco soda_cloud, che governa invece l'upload su Soda Cloud.
    """

    def __init__(self, limit: int = 100):
        self.failed_data = {}
        self.limit = limit

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
                column_names = []

                if hasattr(schema, 'columns'):
                    column_names = [c.name if hasattr(c, 'name') else c.get('name') for c in schema.columns]
                elif hasattr(schema, 'get_dict'):
                    d = schema.get_dict()
                    if 'columns' in d:
                        column_names = [c.get('name') for c in d['columns']]

                if not column_names and rows:
                    column_names = [f"col_{i}" for i in range(len(rows[0]))]

                if rows:
                    safe_rows = rows[:self.limit]
                    self.failed_data[check_name] = [dict(zip(column_names, r)) for r in safe_rows]

        except Exception as e:
            logger.warning(f"Impossibile estrarre le righe in RAM per il check '{check_name}': {e}")

        row_count = len(rows) if rows else 0
        sample_name = getattr(sample_context, 'sample_name', 'failed_rows') or "failed_rows"

        return SampleRef(
            name=sample_name,
            schema=schema,
            total_row_count=row_count,
            stored_row_count=len(self.failed_data.get(check_name, [])),
            type="failed_rows"
        )


def _load_view(
    spark: SparkSession,
    full_name: str,
    view_name: str,
    table_limit: int,
) -> tuple[DataFrame, Optional[DataFrame]]:
    """Carica una tabella, la registra come temp view e (in dev/test con
    table_limit>0) ne materializza in cache una slice limitata.

    Ritorna `(df, persisted)` dove `persisted` è l'handle cachato da liberare a
    fine scan (None in prod, dove table_limit=0 lascia la view lazy così Iceberg
    può fare partition pruning/pushdown sui check incrementali).
    """
    safe_name = ".".join(f"`{part}`" for part in full_name.split("."))
    df = spark.table(safe_name)
    persisted = None

    if table_limit and table_limit > 0:
        # df.limit(...) è lazy: senza persist verrebbe ricalcolato ad ogni check
        # (ogni check Soda è un job Spark separato). Materializziamo le N righe
        # una volta sola in cache; il count() qui sotto la popola.
        df = df.limit(table_limit).persist(StorageLevel.MEMORY_AND_DISK)
        df.count()
        persisted = df

    df.createOrReplaceTempView(view_name)
    return df, persisted


def run_dataframe_soda_scan(
    spark: SparkSession, contract: dict, config: AppConfig
) -> tuple[list[dict], int, MemorySampler]:
    """Esegue la scansione SodaCL e restituisce: check, totale righe e il sampler con i failed_rows."""

    limit_msg = f"(LIMIT {config.table_limit})" if config.table_limit and config.table_limit > 0 else "(NESSUN LIMITE)"
    logger.info(f"Caricamento dataframe per dataset: {contract['dataset']} {limit_msg}")

    # DB principale come fallback per le xref senza prefisso
    main_db = contract["dataset"].split(".")[0] if "." in contract["dataset"] else ""

    # DataFrame messi in cache (solo dev/test con limit): da liberare a fine scan.
    persisted_dfs: list[DataFrame] = []

    try:
        # 1. Dataset principale
        logger.info(f"Materializzazione DataFrame per la vista '{contract['table_name']}'...")
        main_df, persisted = _load_view(
            spark, contract["dataset"], contract["table_name"], config.table_limit
        )
        if persisted is not None:
            persisted_dfs.append(persisted)

        total_rows = main_df.count()
        logger.info(f"Il DataFrame contiene {total_rows} righe che verranno scansionate.")

        # 2. Dataset XREF (cross-reference)
        for xref in contract.get("xref_datasets", []):
            if not xref:
                continue

            xref_full_name = f"{main_db}.{xref}" if "." not in xref and main_db else xref
            xref_table_name = xref.split(".")[-1].replace("-", "_")
            try:
                _, xref_persisted = _load_view(spark, xref_full_name, xref_table_name, config.table_limit)
                if xref_persisted is not None:
                    persisted_dfs.append(xref_persisted)
                logger.info(f"Temp View XREF creata con successo: {xref_table_name} (da {xref_full_name})")
            except Exception as e:
                logger.error(f"Errore durante la creazione della Temp View XREF per '{xref_full_name}': {e}")

    except Exception as e:
        raise ScanExecutionError(
            f"Errore caricamento tabella Spark {contract['dataset']}: {e}"
        ) from e

    logger.info(f"Esecuzione Soda Scan per la vista temporanea '{contract['table_name']}'...")
    logger.info(f"\n{'-'*30} Controlli generati {'-'*30}\n{contract['sodacl']}\n{'-'*88}")

    scan = Scan()

    # Sampler in memoria: intercetta le failed rows on-prem (nessun upload).
    sampler = MemorySampler(limit=config.failed_sample_limit)
    scan.sampler = sampler

    scan.set_data_source_name(config.data_source)
    scan.add_spark_session(spark, data_source_name=config.data_source)
    scan.add_sodacl_yaml_str(contract["sodacl"])

    # Limite a livello di scan: Soda pesca al massimo N failed-row direttamente
    # nella query (LIMIT su Spark) invece di prenderne 100 e troncare a posteriori.
    # 'sampler' non e' un header di config top-level valido (andrebbe annidato sotto
    # 'data_source <nome>', in conflitto con la spark session programmatica): si
    # imposta direttamente il campo letto da Soda in metric.py. Separato dal cloud.
    scan._configuration.samples_limit = config.failed_sample_limit

    soda_api_key    = secrets.soda_api_key()
    soda_api_secret = secrets.soda_api_secret()

    if soda_api_key and soda_api_secret:
        logger.info("Credenziali Soda Cloud rilevate. Invio metriche aggregate attivato (nessun dettaglio record).")
        # samples_limit FISSO a 0 (non configurabile): nessuna failed-row viene mai
        # caricata su Soda Cloud. Il dettaglio-record resta on-prem nel MemorySampler
        # -> tabella Iceberg failed_records.
        soda_cfg = f"""
        soda_cloud:
          host: {config.soda_host}
          api_key_id: {soda_api_key}
          api_key_secret: {soda_api_secret}
          samples_limit: 0
        """
        scan.add_configuration_yaml_str(soda_cfg)
        scan.set_scan_definition_name(contract["contract_title"])
    else:
        logger.warning("Credenziali Soda Cloud mancanti: l'esecuzione avverrà solo in locale.")

    try:
        logger.info(f"Avvio Soda scan.execute() sulla vista '{contract['table_name']}'...")
        scan.execute()
    except Exception as e:
        logger.error(f"Soda scan.execute() fallito per '{contract['table_name']}': {e}", exc_info=True)
        raise
    finally:
        # Libera la cache (se attivata col limit): lo scan è finito.
        for df in persisted_dfs:
            df.unpersist()

    checks = scan.get_scan_results().get("checks", [])
    logger.info(f"Soda scan completato: {len(checks)} check valutati.")

    # scan.execute() NON solleva quando la query di un check va in errore (OOM,
    # executor morto, SQL non valido): Soda registra l'errore nei propri log e
    # scarta l'intero risultato. Senza questo controllo la pipeline proseguirebbe
    # con 0 check e uscirebbe con exit code 0 -> job SUCCEEDED su CDE.
    if scan.has_error_logs():
        raise ScanExecutionError(
            f"Scan Soda fallito su '{contract['table_name']}': una o piu' query di check "
            f"sono andate in errore.\n{scan.get_error_logs_text()}"
        )

    if not checks:
        raise ScanExecutionError(
            f"Scan Soda su '{contract['table_name']}' non ha valutato alcun check: "
            f"nessun esito da scrivere."
        )

    return checks, total_rows, sampler
