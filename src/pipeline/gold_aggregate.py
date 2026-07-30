import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as _sum,
    avg,
    count,
    when,
    col,
    round as spark_round,
    max as _max,
)
from pyspark import pipelines as dp

spark = SparkSession.getActiveSession()
_bundle_root = spark.conf.get("bundle.sourcePath", None)
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from config import azure_settings

SILVER = azure_settings.SILVER_SCHEMA


@dp.materialized_view(name="d_production_summary")
def d_production_summary():
    batches = spark.read.table(f"{SILVER}.fact_production_batches")
    plants = spark.read.table(f"{SILVER}.silver_dim_plants")
    lines = spark.read.table(f"{SILVER}.silver_dim_lines")
    return (
        batches.groupBy("plant_id", "line_id")
        .agg(
            count("batch_id").alias("total_batches"),
            _sum("planned_qty_units").alias("total_planned_units"),
            _sum("actual_qty_units").alias("total_actual_units"),
            _sum("defect_units").alias("total_defect_units"),
            avg("yield_pct").alias("avg_yield_pct"),
            _sum(when(col("batch_status") == "Scrapped", 1).otherwise(0)).alias("scrapped_batches")
        )
        .withColumn(
            "defect_rate_pct",
            when(
                col("total_actual_units") > 0,
                spark_round(col("total_defect_units") / col("total_actual_units") * 100, 2)
            ).otherwise(0.0)
        )
        .join(plants.select("plant_id", "plant_name", "country"), "plant_id")
        .join(lines.select("line_id", "line_type", "line_name"), "line_id")
    )


@dp.materialized_view(name="d_retailer_360")
def d_retailer_360():
    shipments = spark.read.table(f"{SILVER}.fact_shipments")
    customers = spark.read.table(f"{SILVER}.silver_dim_retail_customers")
    return (
        shipments.groupBy("customer_id")
        .agg(
            count("shipment_id").alias("total_shipments"),
            _sum("qty_units").alias("total_units_shipped"),
            _sum(when(col("shipment_status") == "Delivered", 1).otherwise(0)).alias("delivered_count"),
            _sum(when(col("shipment_status") == "Delayed", 1).otherwise(0)).alias("delayed_count"),
            _sum(when(col("shipment_status") == "Returned", 1).otherwise(0)).alias("returned_count")
        )
        .withColumn(
            "on_time_rate_pct",
            when(
                col("total_shipments") > 0,
                spark_round(col("delivered_count") / col("total_shipments") * 100, 2)
            ).otherwise(0.0)
        )
        .join(customers, "customer_id")
    )


@dp.materialized_view(name="d_quality_audit_summary")
def d_quality_audit_summary():
    audits = spark.read.table(f"{SILVER}.fact_quality_audits")
    batches = spark.read.table(f"{SILVER}.fact_production_batches").select("batch_id", "plant_id")
    plants = spark.read.table(f"{SILVER}.silver_dim_plants").select("plant_id", "plant_name")
    return (
        audits.join(batches, "batch_id")
        .join(plants, "plant_id")
        .groupBy("plant_id", "plant_name", "audit_outcome")
        .agg(count("audit_id").alias("audit_count"))
    )


@dp.materialized_view(name="d_sensor_health_summary")
def d_sensor_health_summary():
    sensors = spark.read.table(f"{SILVER}.fact_sensor_readings")
    lines = spark.read.table(f"{SILVER}.silver_dim_lines")
    return (
        sensors.groupBy("line_id", "data_source")
        .agg(
            count("reading_id").alias("total_readings"),
            _sum(col("anomaly_flag")).alias("anomaly_count"),
            avg("temperature_c").alias("avg_temperature_c"),
            _max("temperature_c").alias("max_temperature_c"),
            avg("pressure_bar").alias("avg_pressure_bar"),
            avg("vibration_mm_s").alias("avg_vibration_mm_s"),
            _max("vibration_mm_s").alias("max_vibration_mm_s"),
            _max("event_timestamp").alias("latest_reading_ts")
        )
        .withColumn(
            "anomaly_rate_pct",
            when(
                col("total_readings") > 0,
                spark_round(col("anomaly_count") / col("total_readings") * 100, 2)
            ).otherwise(0.0)
        )
        .join(lines.select("line_id", "line_name", "line_type"), "line_id")
    )