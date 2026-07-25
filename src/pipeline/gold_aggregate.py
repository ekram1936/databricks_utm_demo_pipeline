"""
Gold layer aggregation: builds business-ready summary tables from Silver
into Unity Catalog (utm_demo_catalog.gold).

Run this from a Databricks notebook or job:
    from src.pipeline import gold_aggregate
    gold_aggregate.run(spark)
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run(spark):
    from pyspark.sql.functions import sum as _sum, avg, count, when, col, round as spark_round

    silver = azure_settings.SILVER_SCHEMA
    gold = azure_settings.GOLD_SCHEMA

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")

    logger.info("Building gold.d_production_summary ...")
    batches = spark.table(f"{silver}.fact_production_batches")
    plants = spark.table(f"{silver}.dim_plants")
    lines = spark.table(f"{silver}.dim_lines")

    d_production_summary = batches.groupBy("plant_id", "line_id") \
        .agg(
            count("batch_id").alias("total_batches"),
            _sum("planned_qty_units").alias("total_planned_units"),
            _sum("actual_qty_units").alias("total_actual_units"),
            _sum("defect_units").alias("total_defect_units"),
            avg("yield_pct").alias("avg_yield_pct"),
            _sum(when(col("batch_status") == "Scrapped", 1).otherwise(0)).alias("scrapped_batches")
        ) \
        .withColumn("defect_rate_pct", spark_round(col("total_defect_units") / col("total_actual_units") * 100, 2)) \
        .join(plants.select("plant_id", "plant_name", "country"), "plant_id") \
        .join(lines.select("line_id", "line_type", "line_name"), "line_id")

    d_production_summary.write.mode("overwrite").saveAsTable(f"{gold}.d_production_summary")

    logger.info("Building gold.d_retailer_360 ...")
    shipments = spark.table(f"{silver}.fact_shipments")
    customers = spark.table(f"{silver}.dim_retail_customers")

    d_retailer_360 = shipments.groupBy("customer_id") \
        .agg(
            count("shipment_id").alias("total_shipments"),
            _sum("qty_units").alias("total_units_shipped"),
            _sum(when(col("shipment_status") == "Delivered", 1).otherwise(0)).alias("delivered_count"),
            _sum(when(col("shipment_status") == "Delayed", 1).otherwise(0)).alias("delayed_count"),
            _sum(when(col("shipment_status") == "Returned", 1).otherwise(0)).alias("returned_count")
        ) \
        .withColumn("on_time_rate_pct", spark_round(col("delivered_count") / col("total_shipments") * 100, 2)) \
        .join(customers, "customer_id")

    d_retailer_360.write.mode("overwrite").saveAsTable(f"{gold}.d_retailer_360")

    logger.info("Building gold.d_quality_audit_summary ...")
    audits = spark.table(f"{silver}.fact_quality_audits")
    batch_plant_map = batches.select("batch_id", "plant_id")

    d_quality_audit_summary = audits.join(batch_plant_map, "batch_id") \
        .join(plants.select("plant_id", "plant_name"), "plant_id") \
        .groupBy("plant_id", "plant_name", "audit_outcome") \
        .agg(count("audit_id").alias("audit_count"))

    d_quality_audit_summary.write.mode("overwrite").saveAsTable(f"{gold}.d_quality_audit_summary")

    logger.info("Gold layer aggregation complete. 3 tables written.")
