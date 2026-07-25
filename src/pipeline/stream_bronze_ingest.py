"""
Bronze streaming ingestion: reads live sensor events from Azure Event Hubs
(via its Kafka-compatible endpoint) and writes them as a Delta streaming
table in Unity Catalog (utm_demo_catalog.bronze.sensor_readings_stream).
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

SECRET_SCOPE = "eventhub-secrets"
SECRET_KEY = "connection-string"
CHECKPOINT_PATH_SUFFIX = "_checkpoints/sensor_readings_stream"


def build_kafka_options(dbutils):
    conn_str = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    namespace = azure_settings.EVENT_HUB_NAMESPACE
    eh_name = azure_settings.EVENT_HUB_NAME

    kafka_bootstrap = f"{namespace}.servicebus.windows.net:9093"
    sasl_config = (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" '
        f'password="{conn_str}";'
    )

    return {
        "kafka.bootstrap.servers": kafka_bootstrap,
        "subscribe": eh_name,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": sasl_config,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


def run(spark, dbutils, await_termination: bool = True):
    from pyspark.sql.functions import col, from_json, to_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    logger.info("Configuring Kafka-compatible read from Azure Event Hubs ...")
    kafka_options = build_kafka_options(dbutils)

    raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

    schema = StructType([
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

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*").withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

    target_table = f"{azure_settings.BRONZE_SCHEMA}.sensor_readings_stream"
    checkpoint_path = azure_settings.abfss_path(CHECKPOINT_PATH_SUFFIX)

    logger.info(f"Starting streaming write to {target_table} (checkpoint: {checkpoint_path}) ...")
    query = (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    logger.info(f"Streaming query started: {query.id}")
    if await_termination:
        query.awaitTermination()
        logger.info(f"Streaming batch completed for {target_table}")
    return query