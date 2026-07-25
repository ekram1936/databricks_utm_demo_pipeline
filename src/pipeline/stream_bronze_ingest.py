"""
Bronze streaming ingestion: reads live sensor events from Azure Event Hubs
(via its Kafka-compatible endpoint) and writes them as a Delta streaming
table in Unity Catalog (utm_demo_catalog.bronze.sensor_readings_stream).

Run this from a Databricks notebook:
    from src.pipeline import stream_bronze_ingest
    stream_bronze_ingest.run(spark)

Prerequisites:
1. Store your Event Hubs connection string in a Databricks secret scope:
     databricks secrets create-scope eventhub-secrets
     databricks secrets put-secret eventhub-secrets connection-string
2. Reference it below via dbutils.secrets.get(...)
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
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" '
        f'password="{conn_str}";'
    )

    return {
        "kafka.bootstrap.servers": kafka_bootstrap,
        "subscribe": eh_name,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": sasl_config,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
    }


def run(spark, dbutils, trigger_seconds: int = 30, await_termination: bool = False):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

    logger.info("Configuring Kafka-compatible read from Azure Event Hubs ...")
    kafka_options = build_kafka_options(dbutils)

    raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

    schema = StructType([
        StructField("reading_id", StringType()),
        StructField("line_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("temperature_c", DoubleType()),
        StructField("pressure_bar", DoubleType()),
        StructField("anomaly_flag", IntegerType()),
    ])

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    target_table = f"{azure_settings.BRONZE_SCHEMA}.sensor_readings_stream"
    checkpoint_path = azure_settings.abfss_path(CHECKPOINT_PATH_SUFFIX)

    logger.info(f"Starting streaming write to {target_table} (checkpoint: {checkpoint_path}) ...")
    query = (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .toTable(target_table)
    )

    logger.info(f"Streaming query started: {query.id}")
    if await_termination:
        query.awaitTermination()
    return query
