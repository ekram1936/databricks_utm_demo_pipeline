"""
Silver layer transformations: cleans, deduplicates, and enriches Bronze tables
into Unity Catalog (utm_demo_catalog.silver).

Run this from a Databricks notebook or job:
    from src.pipeline import silver_transform
    silver_transform.run(spark)
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run(spark):
    from pyspark.sql.functions import col, round as spark_round, lit

    bronze = azure_settings.BRONZE_SCHEMA
    silver = azure_settings.SILVER_SCHEMA

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")

    logger.info("Transforming silver.dim_plants ...")
    spark.table(f"{bronze}.dim_plants") \
        .withColumn("capacity_liters_day", col("capacity_liters_day").cast("int")) \
        .withColumn("opened_year", col("opened_year").cast("int")) \
        .dropDuplicates(["plant_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.dim_plants")

    logger.info("Transforming silver.dim_lines ...")
    spark.table(f"{bronze}.dim_lines").dropDuplicates(["line_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.dim_lines")

    logger.info("Transforming silver.dim_sku_catalog ...")
    spark.table(f"{bronze}.dim_sku_catalog").dropDuplicates(["sku_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.dim_sku_catalog")

    logger.info("Transforming silver.dim_retail_customers ...")
    spark.table(f"{bronze}.dim_retail_customers").dropDuplicates(["customer_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.dim_retail_customers")

    logger.info("Transforming silver.fact_production_batches ...")
    spark.table(f"{bronze}.historical_production_batches") \
        .withColumn("planned_qty_units", col("planned_qty_units").cast("int")) \
        .withColumn("actual_qty_units", col("actual_qty_units").cast("int")) \
        .withColumn("defect_units", col("defect_units").cast("int")) \
        .withColumn("yield_pct", spark_round((col("actual_qty_units") - col("defect_units")) / col("actual_qty_units") * 100, 2)) \
        .dropDuplicates(["batch_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.fact_production_batches")

    logger.info("Transforming silver.fact_shipments ...")
    spark.table(f"{bronze}.historical_shipments") \
        .withColumn("qty_units", col("qty_units").cast("int")) \
        .dropDuplicates(["shipment_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.fact_shipments")

    logger.info("Transforming silver.fact_quality_audits ...")
    spark.table(f"{bronze}.historical_quality_audit_logs").dropDuplicates(["audit_id"]) \
        .write.mode("overwrite").saveAsTable(f"{silver}.fact_quality_audits")

    logger.info("Transforming silver.fact_sensor_readings (union of batch + streaming Bronze sources) ...")

    sensor_cols = [
        "reading_id", "line_id", "event_timestamp", "temperature_c",
        "pressure_bar", "fill_volume_ml", "vibration_mm_s",
        "machine_status", "anomaly_flag"
    ]

    def prep_sensor_df(df):
        return (
            df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
              .withColumn("temperature_c", col("temperature_c").cast("double"))
              .withColumn("pressure_bar", col("pressure_bar").cast("double"))
              .withColumn("fill_volume_ml", col("fill_volume_ml").cast("double"))
              .withColumn("vibration_mm_s", col("vibration_mm_s").cast("double"))
              .withColumn("anomaly_flag", col("anomaly_flag").cast("int"))
              .select(*sensor_cols)
        )

    batch_sensor_df = prep_sensor_df(spark.table(f"{bronze}.sensor_readings")) \
        .withColumn("data_source", lit("batch_file"))

    stream_table_exists = spark.catalog.tableExists(f"{bronze}.sensor_readings_stream")

    if stream_table_exists:
        stream_sensor_df = prep_sensor_df(spark.table(f"{bronze}.sensor_readings_stream")) \
            .withColumn("data_source", lit("event_hub_stream"))
        combined_sensor_df = batch_sensor_df.unionByName(stream_sensor_df)
        logger.info("Unioned batch (bronze.sensor_readings) + streaming (bronze.sensor_readings_stream).")
    else:
        combined_sensor_df = batch_sensor_df
        logger.info("bronze.sensor_readings_stream not found yet, using batch source only.")

    combined_sensor_df.dropDuplicates(["reading_id"]) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver}.fact_sensor_readings")

    logger.info("Silver layer transformation complete. 8 tables written.")