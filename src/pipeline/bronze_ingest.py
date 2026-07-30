import sys
from pyspark.sql import SparkSession
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp

spark = SparkSession.getActiveSession()
_bundle_root = spark.conf.get("bundle.sourcePath", None)
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from config.settings import *
from config import azure_settings
from src.utils.logger import get_logger

# --- 4 dimension tables ---

@dp.materialized_view(name="dim_plants")
@dp.expect("valid_plant_id", "plant_id IS NOT NULL")
def dim_plants():
    return spark.read.option("header", "true").csv(azure_settings.abfss_path("dim/dim_plants.csv"))


@dp.materialized_view(name="dim_lines")
def dim_lines():
    return spark.read.option("header", "true").csv(azure_settings.abfss_path("dim/dim_lines.csv"))


@dp.materialized_view(name="dim_sku_catalog")
def dim_sku_catalog():
    return spark.read.option("header", "true").csv(azure_settings.abfss_path("dim/dim_sku_catalog.csv"))


@dp.materialized_view(name="dim_retail_customers")
def dim_retail_customers():
    return spark.read.option("header", "true").csv(azure_settings.abfss_path("dim/dim_retail_customers.csv"))


# --- 3 historical fact tables ---

@dp.materialized_view(name="historical_production_batches")
@dp.expect("valid_batch_id", "batch_id IS NOT NULL")
def historical_production_batches():
    return spark.read.option("header", "true").csv(
        azure_settings.abfss_path("historical/historical_production_batches.csv")
    )


@dp.materialized_view(name="historical_shipments")
def historical_shipments():
    return spark.read.option("header", "true").csv(
        azure_settings.abfss_path("historical/historical_shipments.csv")
    )


@dp.materialized_view(name="historical_quality_audit_logs")
def historical_quality_audit_logs():
    return spark.read.option("header", "true").csv(
        azure_settings.abfss_path("historical/historical_quality_audit_logs.csv")
    )


# --- sensor_readings: batch JSON snapshot ---

@dp.materialized_view(name="sensor_readings")
def sensor_readings():
    return spark.read.json(azure_settings.abfss_path("streaming/"))


# --- sensor_readings_stream: LIVE via Event Hubs Kafka endpoint ---
SENSOR_SCHEMA = StructType([
    StructField("reading_id", StringType()),
    StructField("line_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("pressure_bar", DoubleType()),
    StructField("fill_volume_ml", DoubleType()),
    StructField("vibration_mm_s", DoubleType()),
    StructField("machine_status", StringType()),
    StructField("anomaly_flag", IntegerType()),
])


@dp.table(name="sensor_readings_stream")
@dp.expect("valid_reading_id", "reading_id IS NOT NULL")
@dp.expect_or_drop("plausible_temperature", "temperature_c BETWEEN -20 AND 150")
def sensor_readings_stream():
    conn_str = dbutils.secrets.get(scope="eventhub-secrets", key="connection-string")
    kafka_bootstrap = f"{azure_settings.EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093"
    sasl_config = (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", azure_settings.EVENT_HUB_NAME)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", sasl_config)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw_stream.select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    )