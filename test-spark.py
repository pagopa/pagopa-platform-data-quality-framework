"""
gpd_quality_pipeline.py
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
CONTRACTS_PATH = os.getenv("GPD_CONTRACTS_PATH", "/app/mount/")
TABLE_LIMIT    = int(os.getenv("GPD_TABLE_LIMIT", "50"))
DATA_SOURCE    = os.getenv("GPD_DATA_SOURCE", "my_spark")

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
        
    dataset = list(models.keys())[0].strip()

    info       = doc.get("info", {})
    table_name = dataset.split(".")[-1].replace("-", "_")

    # Generazione SodaCL delegata all'API Python
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

def load_contracts(contracts_path: str) -> list[dict]:
    files = sorted(glob(os.path.join(contracts_path, "**/*.yaml"), recursive=True))
    logger.info(f"Trovati {len(files)} file YAML nel percorso {contracts_path}")
    return [c for f in files if (c := _parse_contract_file(f))]

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

def run_pipeline() -> None:
    logger.info("Avvio pipeline Data Quality GPD")
    contracts = load_contracts(CONTRACTS_PATH)
    
    if not contracts:
        logger.error("Nessun contract valido trovato. Pipeline terminata.")
        return

    spark = init_spark()
    all_rows = []

    for contract in contracts:
        logger.info("+" * 80)
        logger.info(f"Elaborazione Contract: {contract['contract_title']} ({contract['contract_path']})")
        
        checks = _run_soda_scan(spark, contract)
        if not checks:
            logger.warning("Nessun check restituito dallo scan, passo al prossimo.")
            continue

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

    # Consolidamento e output finale
    if all_rows:
        df_results = spark.createDataFrame(all_rows, schema=RESULTS_SCHEMA)
        _log_results_summary(df_results)
    else:
        logger.warning("Nessun risultato elaborato.")

    logger.info("Pipeline completata. Chiusura sessione Spark.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()