"""
Pipeline di data quality per i Data Contract GPD — Silver Layer.
Compatibile con job CDE (Cloudera Data Engineering).

Orchestrazione:
1. Parsing dei metadati dal file YAML (Data Contract).
2. Generazione regole SodaCL tramite libreria Python nativa (datacontract-cli).
3. Esecuzione scansione tramite libreria Soda Core su PySpark.
4. Consolidamento dei risultati in un DataFrame.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime
from glob import glob
from typing import Optional

import yaml
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from soda.scan import Scan

# Importazione della libreria Python nativa di DataContract
try:
    from datacontract.data_contract import DataContract
except ImportError:
    print("ERRORE CRITICO: Libreria 'datacontract' non trovata. Verificare il Virtual Environment CDE.")
    sys.exit(1)

# ── Configurazione Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(module)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── Configurazione Variabili d'Ambiente ────────────────────────────────────────
CONTRACTS_PATH = os.getenv("CONTRACTS_PATH", "/app/mount/")
TABLE_LIMIT    = int(os.getenv("TABLE_LIMIT", "200"))
DATA_SOURCE    = os.getenv("DATA_SOURCE", "ny_spark")

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA DATAFRAME RISULTATI
# ══════════════════════════════════════════════════════════════════════════════

RESULTS_SCHEMA = StructType([
    StructField("data_contract",         StringType(),    True),
    StructField("data_contract_version", StringType(),    True),
    StructField("nome_check",            StringType(),    True),
    StructField("esito",                 StringType(),    True),
    StructField("valore_misurato",       DoubleType(),    True),
    StructField("soglia_warn",           StringType(),    True),
    StructField("soglia_fail",           StringType(),    True),
    StructField("timestamp",             TimestampType(), True),
    StructField("datasource",            StringType(),    True),
    StructField("dataset",               StringType(),    True),
    StructField("num_righe_controllate", LongType(),      True),
])

# ══════════════════════════════════════════════════════════════════════════════
# DATACONTRACT API & LOADER
# Utilizzo dell'API Python di DataContract per convertire in SodaCL
# ══════════════════════════════════════════════════════════════════════════════

def _generate_sodacl(filepath: str) -> str | None:
    """
    Utilizza l'API Python della libreria `datacontract` per leggere il file YAML
    ed esportarlo in formato SodaCL.
    """
    logger.info(f"Avvio conversione DataContract -> SodaCL per: {filepath}")
    try:
        data_contract = DataContract(data_contract_file=filepath)
        sodacl_string = data_contract.export(export_format="sodacl")
        
        logger.info("Conversione riuscita con successo!")
        logger.info(f"\n{'-'*30} CONTROLLI GENERATI (DEBUG) {'-'*30}\n{sodacl_string}\n{'-'*88}")
        
        return sodacl_string

    except Exception as e:
        logger.error(f"Errore durante la conversione tramite DataContract API: {str(e)}")
        return None

def _normalize_sodacl(sodacl: str, dataset: str, table_name: str) -> str:
    """Assicura che il nome tabella in SodaCL corrisponda al nome della vista temporanea Spark."""
    normalized = re.sub(
        r"checks for [^\s:]+:",
        f"checks for {table_name}:",
        sodacl,
    )
    return normalized.replace(dataset, table_name)

def _parse_contract_file(filepath: str) -> dict | None:
    """Legge metadati essenziali dallo YAML e delega la generazione del codice all'API."""
    try:
        with open(filepath, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception as e:
        logger.error(f"Impossibile leggere il file YAML {filepath}: {e}")
        return None

    # Controllo severo: se non è uno standard valido, scarta.
    if "dataContractSpecification" not in doc or "models" not in doc:
        logger.error(f"File saltato '{filepath}': File non valido. Manca 'dataContractSpecification' o 'models'.")
        return None

    # Estrazione corretta del dataset dallo standard (prendiamo il primo modello definito)
    models = doc.get("models", {})
    if not models:
        logger.error(f"File saltato '{filepath}': Il tag 'models' è presente ma vuoto.")
        return None
        
    model_name = list(models.keys())[0].strip()
    # Legge il dataset reale, se non c'è usa il nome del modello come fallback
    dataset = models[model_name].get("dataset", model_name).strip()

    info       = doc.get("info", {})
    table_name = dataset.split(".")[-1].replace("-", "_")

    # Generazione SodaCL delegata all'API Python
    

    ignore_cli = os.getenv("IGNORE_DATACONTRACT_CLI", "false").lower() == "true"

    if ignore_cli:
        
        soda_filepath = "./dq_framework/tests/fixtures/contracts/soda.yml"
        logger.info(f"IGNORE_DATACONTRACT_CLI=true: Lettura diretta dei controlli dal file {soda_filepath}")
        try:
            with open(soda_filepath, "r", encoding="utf-8") as f:
                raw_sodacl = f.read()
            
            logger.info("Lettura da soda.yml riuscita con successo!")
            logger.info(f"\n{'-'*30} CONTROLLI CARICATI (DEBUG) {'-'*30}\n{raw_sodacl}\n{'-'*88}")
        except Exception as e:
            logger.error(f"Errore durante la lettura del file {soda_filepath}: {str(e)}")
    else:
        raw_sodacl = _generate_sodacl(filepath)


    if not raw_sodacl:
        logger.warning(f"File saltato '{filepath}': generazione SodaCL fallita.")
        return None

    return {
        "contract_path":    filepath,
        "contract_title":   info.get("title", os.path.basename(filepath)),
        "contract_version": str(info.get("version", "")),
        "dataset":          dataset,
        "table_name":       table_name,
        "sodacl":           _normalize_sodacl(raw_sodacl, dataset, table_name),
    }

# ══════════════════════════════════════════════════════════════════════════════
# PROCESSOR SODA RESULTS
# ══════════════════════════════════════════════════════════════════════════════

def _extract_row_count(checks: list[dict]) -> Optional[int]:
    """Cerca il valore del row_count nei check processati."""
    for chk in checks:
        metrics = chk.get("metrics", []) or []
        if any("row_count" in str(m) for m in metrics):
            val = (chk.get("diagnostics") or {}).get("value")
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass
    return None

def process_scan_results(
    scan_checks:      list[dict],
    contract_title:   str,
    contract_version: str,
    table_name:       str,
    scan_ts:          datetime,
    data_source:      str,
) -> list[Row]:
    """Elabora i risultati dello scan Soda trasformandoli in Row PySpark."""
    row_count = _extract_row_count(scan_checks)
    rows: list[Row] = []

    for chk in scan_checks:
        diag     = chk.get("diagnostics") or {}
        esito    = chk.get("outcome", "")
        
        try:
            measured = float(diag.get("value")) if diag.get("value") is not None else None
        except (TypeError, ValueError):
            measured = None

        warn_dict  = diag.get("warn")
        fail_dict  = diag.get("fail")
        check_name = chk.get("name") or chk.get("definition", "")

        rows.append(Row(
            data_contract         = contract_title,
            data_contract_version = contract_version,
            nome_check            = check_name[:80],
            esito                 = esito,
            valore_misurato       = measured,
            soglia_warn           = str(warn_dict) if warn_dict else None,
            soglia_fail           = str(fail_dict) if fail_dict else None,
            timestamp             = scan_ts,
            datasource            = chk.get("dataSource", data_source),
            dataset               = chk.get("table", table_name),
            num_righe_controllate = row_count,
        ))

    return rows

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def init_spark() -> SparkSession:
    logger.info("Inizializzazione SparkSession...")
    return (
        SparkSession.builder
        .appName("gpd_quality_pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )

def _run_soda_scan(spark: SparkSession, contract: dict) -> list[dict]:
    """Esegue la scansione SodaCL sulla tabella Spark e restituisce i check."""
    logger.info(f"Caricamento dataframe per dataset: {contract['dataset']} (LIMIT {TABLE_LIMIT})")
    
    try:
        safe_dataset = ".".join([f"`{part}`" for part in contract["dataset"].split(".")])
        
        df = spark.table(safe_dataset).limit(TABLE_LIMIT)
        df.createOrReplaceTempView(contract["table_name"])
    except Exception as e:
        logger.error(f"Errore caricamento tabella Spark {contract['dataset']}: {e}")
        return []
    
    logger.info(f"Esecuzione Soda Scan per la vista temporanea '{contract['table_name']}'...")
    scan = Scan()
    scan.set_data_source_name(DATA_SOURCE)
    scan.add_spark_session(spark, data_source_name=DATA_SOURCE)
    scan.add_sodacl_yaml_str(contract["sodacl"])

    # ──> CONFIGURAZIONE SODA CLOUD <──
    soda_api_key = os.getenv("SODA_API_KEY")
    soda_api_secret = os.getenv("SODA_API_SECRET")
    soda_host = os.getenv("SODA_HOST", "cloud.soda.io")

    if soda_api_key and soda_api_secret:
        logger.info("Credenziali Soda Cloud rilevate: invio risultati in corso...")
        soda_cfg = f"""
        soda_cloud:
          host: {soda_host}
          api_key_id: {soda_api_key}
          api_key_secret: {soda_api_secret}
          samples_limit: 100
        """
        scan.add_configuration_yaml_str(soda_cfg)
        
        # Obbligatorio per Cloud: diamo un nome al workflow/dataset sulla dashboard
        scan.set_scan_definition_name(contract['contract_title'])
    else:
        logger.warning("Credenziali Soda Cloud mancanti: l'esecuzione avverrà solo in locale.")

    scan.execute()

    return scan.get_scan_results().get("checks", [])

def _log_contract_summary(checks: list[dict], contract_title: str) -> None:
    """Logga il riepilogo di un singolo contract in modo strutturato."""
    passed = [c for c in checks if c.get("outcome") == "pass"]
    warned = [c for c in checks if c.get("outcome") == "warn"]
    failed = [c for c in checks if c.get("outcome") == "fail"]
    errors = [c for c in checks if c.get("outcome") == "error"]

    logger.info(f"Riepilogo contract [{contract_title}] - Totale: {len(checks)} | PASS: {len(passed)} | WARN: {len(warned)} | FAIL: {len(failed)} | ERRORS: {len(errors)}")
    
    for chk in warned + failed + errors:
        diag = chk.get("diagnostics") or {}
        outcome = chk.get("outcome", "unknown").upper()
        logger.warning(f"[{outcome}] Check: {chk.get('name')} | Valore Rilevato: {diag.get('value')}")

def _log_results_summary(df_results) -> None:
    """Logga i risultati finali consolidati, pronti per audit."""
    logger.info("=" * 80)
    logger.info("RISULTATI FINALI PIPELINE DATA QUALITY")
    logger.info("=" * 80)
    
    for row in df_results.collect():
        outcome_tag = f"[{row.esito.upper()}]"
        
        logger.info(f"{outcome_tag} {row.nome_check}")
        logger.info(f"    - Contract : {row.data_contract} v{row.data_contract_version}")
        logger.info(f"    - Dataset  : {row.dataset} (Datasource: {row.datasource})")
        logger.info(f"    - Misura   : Valore={row.valore_misurato} | Righe={row.num_righe_controllate}")
    logger.info("=" * 80)

def run_pipeline(contract_file_path: str) -> None:
    logger.info(f"Avvio pipeline Data Quality GPD per il file: {contract_file_path}")
    
    contract = _parse_contract_file(contract_file_path)
    
    if not contract:
        logger.error("Contract non valido o non trovato. Pipeline terminata.")
        return

    spark = init_spark()
    all_rows = []

    logger.info("+" * 80)
    logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")
    
    checks = _run_soda_scan(spark, contract)
    if checks:
        scan_ts = datetime.utcnow()
        rows = process_scan_results(
            scan_checks      = checks,
            contract_title   = contract["contract_title"],
            contract_version = contract["contract_version"],
            table_name       = contract["table_name"],
            scan_ts          = scan_ts,
            data_source      = DATA_SOURCE,
        )
        
        all_rows.extend(rows)
        _log_contract_summary(checks, contract["contract_title"])
    else:
        logger.warning("Nessun check restituito dallo scan.")

    # Consolidamento e output finale
    if all_rows:
        df_results = spark.createDataFrame(all_rows, schema=RESULTS_SCHEMA)
        _log_results_summary(df_results)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Errore: percorso del file Data Contract mancante.")
        sys.exit(1)
        
    contract_file_arg = sys.argv[1]
    run_pipeline(contract_file_arg)