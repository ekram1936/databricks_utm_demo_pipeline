"""
Bronze layer ingestion: reads raw files from ADLS Gen2 and writes them as
managed Delta tables in Unity Catalog (utm_demo_catalog.bronze).

Run this from a Databricks notebook or job:
    from src.pipeline import bronze_ingest
    bronze_ingest.run(spark)
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

CSV_TABLES = {
    "dim_plants": "dim/dim_plants.csv",
    "dim_lines": "dim/dim_lines.csv",
    "dim_sku_catalog": "dim/dim_sku_catalog.csv",
    "dim_retail_customers": "dim/dim_retail_customers.csv",
    "historical_production_batches": "historical/historical_production_batches.csv",
    "historical_shipments": "historical/historical_shipments.csv",
    "historical_quality_audit_logs": "historical/historical_quality_audit_logs.csv",
}

JSON_TABLES = {
    "sensor_readings": "streaming/",
}


def ingest_csv_table(spark, table_name: str, relative_path: str):
    path = azure_settings.abfss_path(relative_path)
    logger.info(f"Reading CSV for bronze.{table_name} from {path}")
    df = spark.read.option("header", "true").csv(path)
    target = f"{azure_settings.BRONZE_SCHEMA}.{table_name}"
    df.write.mode("overwrite").saveAsTable(target)
    logger.info(f"Wrote {df.count()} rows to {target}")
    return df.count()


def ingest_json_table(spark, table_name: str, relative_path: str):
    path = azure_settings.abfss_path(relative_path)
    logger.info(f"Reading JSON for bronze.{table_name} from {path}")
    df = spark.read.json(path)
    target = f"{azure_settings.BRONZE_SCHEMA}.{table_name}"
    df.write.mode("overwrite").saveAsTable(target)
    logger.info(f"Wrote {df.count()} rows to {target}")
    return df.count()


def run(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {azure_settings.BRONZE_SCHEMA}")

    total_rows = 0
    for table_name, rel_path in CSV_TABLES.items():
        total_rows += ingest_csv_table(spark, table_name, rel_path)

    for table_name, rel_path in JSON_TABLES.items():
        total_rows += ingest_json_table(spark, table_name, rel_path)

    logger.info(f"Bronze ingestion complete. Total rows across all tables: {total_rows}")
    return total_rows
