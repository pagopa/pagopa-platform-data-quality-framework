import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, BooleanType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_mock_data(csv_path):
    logger.info("Creazione ambiente locale PySpark (Hive Metastore Locale)...")
    
    spark = SparkSession.builder \
        .appName("mock_data_setup") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS pagopa")

    # 1. Definiamo uno schema "piatto" per leggere correttamente il CSV
    flat_csv_schema = StructType([
        StructField("dl_id", IntegerType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("dl_ingestion_tms", LongType(), True),
        StructField("dl_event_tms", TimestampType(), True),
        StructField("before", StringType(), True),
        # Colonne piatte per la struttura 'after'
        StructField("after_id", LongType(), True),
        StructField("after_payment_position_id", LongType(), True),
        StructField("after_amount", LongType(), True),
        StructField("after_description", StringType(), True),
        StructField("after_fee", LongType(), True),
        StructField("after_inserted_date", LongType(), True),
        StructField("after_is_partial_payment", BooleanType(), True),
        StructField("after_iuv", StringType(), True),
        StructField("after_organization_fiscal_code", StringType(), True),
        StructField("after_status", StringType(), True),
        StructField("after_validity_date", LongType(), True),
        StructField("after_due_date", LongType(), True),
        StructField("after_retention_date", LongType(), True),
        StructField("after_last_updated_date_notification_fee", LongType(), True)
    ])

    logger.info(f"Lettura dei dati dal file {csv_path}...")
    
    # 2. Leggiamo il DataFrame dal CSV piatto
    df_flat = spark.read.csv(csv_path, header=True, schema=flat_csv_schema)
    
    # 3. Ristrutturiamo il DataFrame per ricreare la struct 'after' nidificata
    df_nested = df_flat.select(
        col("dl_id"),
        col("op"),
        col("ts_ms"),
        col("ts_us"),
        col("ts_ns"),
        col("dl_ingestion_tms"),
        col("dl_event_tms"),
        col("before"),
        struct(
            col("after_id").alias("id"),
            col("after_payment_position_id").alias("payment_position_id"),
            col("after_amount").alias("amount"),
            col("after_description").alias("description"),
            col("after_fee").alias("fee"),
            col("after_inserted_date").alias("inserted_date"),
            col("after_is_partial_payment").alias("is_partial_payment"),
            col("after_iuv").alias("iuv"),
            col("after_organization_fiscal_code").alias("organization_fiscal_code"),
            col("after_status").alias("status"),
            col("after_validity_date").alias("validity_date"),
            col("after_due_date").alias("due_date"),
            col("after_retention_date").alias("retention_date"),
            col("after_last_updated_date_notification_fee").alias("last_updated_date_notification_fee")
        ).alias("after")
    )
    
    # Salva il dataframe come tabella Hive
    df_nested.write.mode("overwrite").saveAsTable("pagopa.silver_gpd_payment_option")
    
    logger.info("Tabella 'pagopa.silver_gpd_payment_option' creata con successo in locale dal file CSV!")
    spark.stop()

if __name__ == "__main__":
    setup_mock_data("./tests/fixtures/data/mock_silver_gpd_payment_option.csv")