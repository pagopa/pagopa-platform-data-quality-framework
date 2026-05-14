import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_payment_option(spark: SparkSession, csv_path: str):
    logger.info(f"Generazione mock data per PAYMENT OPTION dal file: {csv_path}")
    
    flat_csv_schema = StructType([
        StructField("dl_id", IntegerType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("dl_ingestion_tms", LongType(), True),
        StructField("dl_event_tms", TimestampType(), True), 
        StructField("before", StringType(), True),
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

    df_flat = spark.read.csv(csv_path, header=True, schema=flat_csv_schema)
    
    # Creiamo la nidificazione per 'after' e una struct dummy coerente per 'before'
    # (Poiché il CSV piatto non ha i campi before_*, li inizializziamo a null col tipo corretto)
    df_nested = df_flat.select(
        col("dl_id"), col("op"), col("ts_ms"), col("ts_us"), col("ts_ns"),
        col("dl_ingestion_tms"), col("dl_event_tms"),
        
        struct(
            lit(None).cast("int").alias("id"),
            lit(None).cast("int").alias("payment_position_id"),
            lit(None).cast("long").alias("amount"),
            lit(None).cast("string").alias("description"),
            lit(None).cast("long").alias("due_date"),
            lit(None).cast("int").alias("fee"),
            lit(None).cast("long").alias("inserted_date"),
            lit(None).cast("boolean").alias("is_partial_payment"),
            lit(None).cast("string").alias("iuv"),
            lit(None).cast("string").alias("organization_fiscal_code"),
            lit(None).cast("int").alias("notification_fee")
        ).alias("before"),

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
    
    df_nested.write.mode("overwrite").saveAsTable("pagopa.silver_gpd_payment_option")
    logger.info("Tabella 'pagopa.silver_gpd_payment_option' creata con successo!")


def setup_payment_position(spark: SparkSession, csv_path: str):
    logger.info(f"Generazione mock data per PAYMENT POSITION dal file: {csv_path}")
    
    flat_csv_schema = StructType([
        StructField("dl_id", IntegerType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("dl_ingestion_tms", LongType(), True),
        StructField("dl_event_tms", TimestampType(), True),
        StructField("before", StringType(), True),
        StructField("after_id", LongType(), True),
        StructField("after_iupd", StringType(), True),
        StructField("after_fiscal_code", StringType(), True),
        StructField("after_postal_code", StringType(), True),
        StructField("after_province", StringType(), True),
        StructField("after_max_due_date", LongType(), True),
        StructField("after_min_due_date", LongType(), True),
        StructField("after_organization_fiscal_code", StringType(), True),
        StructField("after_company_name", StringType(), True),
        StructField("after_publish_date", LongType(), True),
        StructField("after_region", StringType(), True),
        StructField("after_status", StringType(), True),
        StructField("after_type", StringType(), True),
        StructField("after_validity_date", LongType(), True),
        StructField("after_switch_to_expired", BooleanType(), True),
        StructField("after_payment_date", LongType(), True),
        StructField("after_last_updated_date", LongType(), True),
        StructField("after_inserted_date", LongType(), True),
        StructField("after_service_type", StringType(), True)
    ])

    df_flat = spark.read.csv(csv_path, header=True, schema=flat_csv_schema)
    
    df_nested = df_flat.select(
        col("dl_id"), col("op"), col("ts_ms"), col("ts_us"), col("ts_ns"),
        col("dl_ingestion_tms"), col("dl_event_tms"),
        
        struct(
            lit(None).cast("int").alias("id"),
            lit(None).cast("string").alias("iupd"),
            lit(None).cast("string").alias("fiscal_code"),
            lit(None).cast("long").alias("max_due_date"),
            lit(None).cast("long").alias("min_due_date"),
            lit(None).cast("string").alias("organization_fiscal_code"),
            lit(None).cast("string").alias("company_name"),
            lit(None).cast("string").alias("type"),
            lit(None).cast("boolean").alias("switch_to_expired"),
            lit(None).cast("long").alias("last_updated_date"),
            lit(None).cast("long").alias("inserted_date"),
            lit(None).cast("string").alias("service_type")
        ).alias("before"),

        struct(
            col("after_id").alias("id"),
            col("after_iupd").alias("iupd"),
            col("after_fiscal_code").alias("fiscal_code"),
            col("after_postal_code").alias("postal_code"),
            col("after_province").alias("province"),
            col("after_max_due_date").alias("max_due_date"),
            col("after_min_due_date").alias("min_due_date"),
            col("after_organization_fiscal_code").alias("organization_fiscal_code"),
            col("after_company_name").alias("company_name"),
            col("after_publish_date").alias("publish_date"),
            col("after_region").alias("region"),
            col("after_status").alias("status"),
            col("after_type").alias("type"),
            col("after_validity_date").alias("validity_date"),
            col("after_switch_to_expired").alias("switch_to_expired"),
            col("after_payment_date").alias("payment_date"),
            col("after_last_updated_date").alias("last_updated_date"),
            col("after_inserted_date").alias("inserted_date"),
            col("after_service_type").alias("service_type")
        ).alias("after")
    )
    
    df_nested.write.mode("overwrite").saveAsTable("pagopa.silver_gpd_payment_position")
    logger.info("Tabella 'pagopa.silver_gpd_payment_position' creata con successo!")


def main():
    logger.info("Avvio ambiente PySpark locale (Hive Metastore)...")
    
    spark = SparkSession.builder \
        .appName("mock_data_setup_all") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS pagopa")

    option_csv_path = "./dq_framework/tests/fixtures/data/mock_silver_gpd_payment_option.csv"
    position_csv_path = "./dq_framework/tests/fixtures/data/mock_silver_gpd_payment_position.csv"

    try:
        setup_payment_option(spark, option_csv_path)
        setup_payment_position(spark, position_csv_path)
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati mock: {e}", exc_info=True)
    finally:
        logger.info("Chiusura della sessione Spark in corso...")
        spark.stop()
        logger.info("Setup dati mock completato.")


if __name__ == "__main__":
    main()
