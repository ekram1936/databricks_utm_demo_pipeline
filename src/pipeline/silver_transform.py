from pyspark import pipelines as dp
from pyspark.sql.functions import col, round as spark_round, lit
import sys
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
_bundle_root = spark.conf.get("bundle.sourcePath", None)
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from config.settings import *
from config import azure_settings

BRONZE = azure_settings.BRONZE_SCHEMA


@dp.materialized_view(name="silver_dim_plants")
def silver_dim_plants():
    return (
        spark.read.table(f"{BRONZE}.dim_plants")
        .withColumn("capacity_liters_day", col("capacity_liters_day").cast("int"))
        .withColumn("opened_year", col("opened_year").cast("int"))
        .dropDuplicates(["plant_id"])
    )


@dp.materialized_view(name="silver_dim_lines")
def silver_dim_lines():
    return spark.read.table(f"{BRONZE}.dim_lines").dropDuplicates(["line_id"])


@dp.materialized_view(name="silver_dim_sku_catalog")
def silver_dim_sku_catalog():
    return spark.read.table(f"{BRONZE}.dim_sku_catalog").dropDuplicates(["sku_id"])


@dp.materialized_view(name="silver_dim_retail_customers")
def silver_dim_retail_customers():
    return spark.read.table(f"{BRONZE}.dim_retail_customers").dropDuplicates(["customer_id"])


@dp.materialized_view(name="fact_production_batches")
def fact_production_batches():
    return (
        spark.read.table(f"{BRONZE}.historical_production_batches")
        .withColumn("planned_qty_units", col("planned_qty_units").cast("int"))
        .withColumn("actual_qty_units", col("actual_qty_units").cast("int"))
        .withColumn("defect_units", col("defect_units").cast("int"))
        .withColumn(
            "yield_pct",
            spark_round(
                (col("actual_qty_units") - col("defect_units")) / col("actual_qty_units") * 100,
                2,
            ),
        )
        .dropDuplicates(["batch_id"])
    )


@dp.materialized_view(name="fact_shipments")
def fact_shipments():
    return (
        spark.read.table(f"{BRONZE}.historical_shipments")
        .withColumn("qty_units", col("qty_units").cast("int"))
        .dropDuplicates(["shipment_id"])
    )


@dp.materialized_view(name="fact_quality_audits")
def fact_quality_audits():
    return spark.read.table(f"{BRONZE}.historical_quality_audit_logs").dropDuplicates(["audit_id"])


SENSOR_COLS = [
    "reading_id", "line_id", "event_timestamp", "temperature_c",
    "pressure_bar", "fill_volume_ml", "vibration_mm_s",
    "machine_status", "anomaly_flag", "data_source"
]


def _prep(df, source_tag):
    return (
        df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
          .withColumn("temperature_c", col("temperature_c").cast("double"))
          .withColumn("pressure_bar", col("pressure_bar").cast("double"))
          .withColumn("fill_volume_ml", col("fill_volume_ml").cast("double"))
          .withColumn("vibration_mm_s", col("vibration_mm_s").cast("double"))
          .withColumn("anomaly_flag", col("anomaly_flag").cast("int"))
          .withColumn("data_source", lit(source_tag))
          .select(*SENSOR_COLS)
    )


@dp.materialized_view(name="fact_sensor_readings")
def fact_sensor_readings():
    batch_df = _prep(
        spark.read.table(f"{BRONZE}.sensor_readings"),
        "batch_file"
    )

    stream_df = _prep(
        spark.read.table(f"{BRONZE}.sensor_readings_stream"),
        "eventhub_stream"
    )

    return (
        batch_df.unionByName(stream_df, allowMissingColumns=True)
        .dropDuplicates(["reading_id"])
    )