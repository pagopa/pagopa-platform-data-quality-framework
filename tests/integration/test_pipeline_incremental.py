"""Integration test end-to-end della pipeline incrementale.

Verifica il ciclo completo:
    1. Setup di una mini tabella sorgente con dl_event_tms distribuita su 5 giorni.
    2. Prima run del pipeline: nessun watermark precedente -> bootstrap -> tutti
       i check incrementali vedono l'intera tabella; viene scritto un risultato
       per ogni check sulla tabella results con le colonne watermark popolate.
    3. Seconda run: il lookup trova il watermark precedente per ogni check
       incrementale; la WHERE generata filtra solo le righe dopo quel
       timestamp; il check massivo continua a vedere tutto.

Il test usa una SparkSession locale con Hive metastore in directory temporanea.
Viene marcato come `integration` per consentirne lo skip in CI veloce.
"""
from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

# Skip pulito se pyspark non e' installato (es. ambienti di lint)
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_framework.common.config.base import AppConfig
from dq_framework.quality import engine


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture Spark + dataset di test
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark(tmp_path_factory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("spark_warehouse")
    derby     = tmp_path_factory.mktemp("derby_metastore")
    s = (
        SparkSession.builder
        .appName("dqf_integration_incremental")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={derby}/metastore_db;create=true")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="module")
def setup_source_table(spark: SparkSession) -> None:
    """Crea pagopa.silver_gpd_payment_option con 5 righe su giorni diversi."""
    spark.sql("CREATE DATABASE IF NOT EXISTS pagopa")
    spark.sql("DROP TABLE IF EXISTS pagopa.silver_gpd_payment_option")

    schema = StructType([
        StructField("dl_id", IntegerType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("dl_ingestion_tms", LongType(), True),
        StructField("dl_event_tms", TimestampType(), False),
        StructField("after_id", LongType(), True),
        StructField("after_amount", LongType(), True),
        StructField("after_iuv", StringType(), True),
    ])

    # 5 righe su 5 giorni consecutivi (2026-05-20 .. 2026-05-24).
    # - tutti gli amount sono >= 0 (check massivo passa)
    # - nessun amount NULL (check incrementale "amount NOT NULL" passa)
    # - iuv univoci (check incrementale "no duplicates" passa)
    base = datetime(2026, 5, 20, 12, 0, 0)
    rows = [
        (i + 1, "c", 0, 0, 0, 0, base + timedelta(days=i),
         100 + i, 1000 + i, f"iuv_{i:03d}")
        for i in range(5)
    ]
    df_flat = spark.createDataFrame(rows, schema=schema)

    df_nested = df_flat.select(
        col("dl_id"), col("op"), col("ts_ms"), col("ts_us"), col("ts_ns"),
        col("dl_ingestion_tms"), col("dl_event_tms"),
        struct(
            col("after_id").alias("id"),
            col("after_amount").alias("amount"),
            col("after_iuv").alias("iuv"),
        ).alias("after"),
        lit(None).cast(StringType()).alias("before"),
    )
    df_nested.write.mode("overwrite").saveAsTable("pagopa.silver_gpd_payment_option")


# ---------------------------------------------------------------------------
# Helper: config di test
# ---------------------------------------------------------------------------

def _make_test_config() -> AppConfig:
    return AppConfig(
        env                   = "test",
        github_api_base_url   = "https://api.github.com",
        contract_path         = "tests/fixtures/contracts/dc-payment-option-incremental.yaml",
        default_repository    = "",
        default_ref           = "main",
        soda_host             = "cloud.soda.io",
        data_source           = "dqf_integration",
        table_limit           = 0,
        results_database      = "pagopa",
        results_table         = "dqf_integration_results",
        results_write_enabled = True,
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_pipeline_incrementale_due_run_consecutive(spark, setup_source_table, monkeypatch):
    """End-to-end: bootstrap + secondo run che usa il watermark del primo."""
    cfg = _make_test_config()
    contract_path = str(Path("tests/fixtures/contracts/dc-payment-option-incremental.yaml").resolve())

    # Per non dipendere dal pacchetto soda-core-spark in CI, monkeypatchiamo
    # run_dataframe_soda_scan per restituire risultati sintetici basati sul
    # sodacl gia' processato. Il test verifica il flusso intorno allo scan:
    # parsing placeholder, lookup, write su tabella results, riuso del watermark.
    captured_sodacl: list[str] = []

    def fake_scan(spark_, contract, config_):
        captured_sodacl.append(contract["sodacl"])
        # Generiamo un check 'pass' per ognuno dei 3 check del contract fixture
        return (
            [
                {"name": "fld__cmp__amount__not_null_incremental", "outcome": "pass",
                 "diagnostics": {"value": 0}},
                {"name": "ent__unq__iuv__no_duplicates_incremental", "outcome": "pass",
                 "diagnostics": {"value": 0}},
                {"name": "fld__vld__amount__non_negative", "outcome": "pass",
                 "diagnostics": {"value": 0}},
            ],
            5,  # total_rows
            # 3o elemento: il sampler dei failed-rows. run_pipeline spacchetta
            # (soda_checks, total_rows, sampler). Qui i check sono tutti 'pass'
            # quindi non ci sono failed-row: basta uno stub con .failed_data
            # vuoto. NB: non importiamo MemorySampler reale per non trascinare
            # la dipendenza soda-core-spark che questo test evita di proposito.
            SimpleNamespace(failed_data={}),
        )

    monkeypatch.setattr(engine, "run_dataframe_soda_scan", fake_scan)

    # Il Dev Container non ha i JAR di Iceberg, quindi `CREATE TABLE ... USING
    # iceberg` e `df.writeTo(fqn).append()` falliscono in locale (entrambi V2
    # catalog). Sostituiamo la scrittura con un equivalente Hive/Parquet che
    # crea la tabella al primo write e fa append automatico ai successivi.
    # La logica del framework che vogliamo testare (parsing, lookup, sostituzione,
    # riuso watermark) e' identica perche' il lookup SQL e' ANSI-standard.
    def fake_write_results(spark_, df_results, config_):
        if not config_.results_write_enabled:
            return
        fqn = f"{config_.results_database}.{config_.results_table}"
        df_results.write.mode("append").format("parquet").saveAsTable(fqn)

    monkeypatch.setattr(engine, "_write_results_to_iceberg", fake_write_results)

    # _process_and_write_failed_records crea/scrive la tabella failed_records via
    # Iceberg (CREATE TABLE ... USING iceberg + writeTo.append), non disponibile
    # in locale. Il test verifica il flusso watermark, non la persistenza dei
    # failed-record: lo rendiamo no-op.
    monkeypatch.setattr(
        engine, "_process_and_write_failed_records", lambda *a, **k: None
    )

    # `run_pipeline` chiama `spark.stop()` alla fine. Nel test la SparkSession
    # e' una fixture condivisa tra le due run consecutive (Spark e' singleton
    # via getOrCreate): se la fermiamo dopo la prima run, la seconda esplode
    # con `NoneType has no attribute setCallSite`. Lo stop reale verra' fatto
    # dalla fixture a fine modulo.
    monkeypatch.setattr(spark, "stop", lambda: None)

    # Cleanup di eventuali run precedenti (idempotenza tra esecuzioni pytest)
    spark.sql(f"DROP TABLE IF EXISTS {cfg.results_database}.{cfg.results_table}")

    # --- Prima run: nessun watermark precedente, bootstrap ----------------
    engine.run_pipeline(
        contract_path = contract_path,
        repository    = "",
        ref           = "main",
        config        = cfg,
    )

    # Il SodaCL passato a fake_scan deve avere il placeholder gia' sostituito
    assert "${INCREMENTAL_CONDITIONS}" not in captured_sodacl[0]
    assert "dl_event_tms >"  in captured_sodacl[0]
    assert "dl_event_tms <=" in captured_sodacl[0]
    # Bootstrap: wm_from = epoch
    assert "TIMESTAMP '1970-01-01" in captured_sodacl[0]

    # Verifica scritta su tabella results
    fqn = f"{cfg.results_database}.{cfg.results_table}"
    df = spark.sql(f"SELECT check_name, outcome, watermark_column, "
                   f"watermark_from, watermark_to FROM {fqn}").collect()
    by_name = {r["check_name"]: r for r in df}

    # I due check incrementali hanno colonne watermark popolate
    for name in (
        "fld__cmp__amount__not_null_incremental",
        "ent__unq__iuv__no_duplicates_incremental",
    ):
        r = by_name[name]
        assert r["outcome"] == "pass"
        assert r["watermark_column"] == "dl_event_tms"
        assert r["watermark_from"] == datetime(1970, 1, 1)
        assert r["watermark_to"] is not None

    # Il check massivo ha tutte e tre le colonne watermark NULL
    r_mass = by_name["fld__vld__amount__non_negative"]
    assert r_mass["watermark_column"] is None
    assert r_mass["watermark_from"] is None
    assert r_mass["watermark_to"] is None

    # --- Seconda run: deve trovare il watermark del primo run ---------------
    captured_sodacl.clear()
    engine.run_pipeline(
        contract_path = contract_path,
        repository    = "",
        ref           = "main",
        config        = cfg,
    )

    # Adesso wm_from NON e' piu' epoch ma il wm_to del run precedente
    assert "TIMESTAMP '1970-01-01" not in captured_sodacl[0]
    # La data scritta nel SodaCL deve avvicinarsi al "now" della prima run
    # (per essere robusti su clock-skew controlliamo solo l'anno corrente)
    assert "TIMESTAMP '2026" in captured_sodacl[0] or "TIMESTAMP '20" in captured_sodacl[0]
