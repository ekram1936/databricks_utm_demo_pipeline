from pyspark import pipelines as dp
from pyspark.sql.functions import col, round as spark_round, lit


@dp.materialized_view(name="silver_dim_plants")
def silver_dim_plants():
    return (
        spark.read.table("dim_plants")
        .withColumn("capacity_liters_day", col("capacity_liters_day").cast("int"))
        .withColumn("opened_year", col("opened_year").cast("int"))
        .dropDuplicates(["plant_id"])
    )


@dp.materialized_view(name="silver_dim_lines")
def silver_dim_lines():
    return spark.read.table("dim_lines").dropDuplicates(["line_id"])


@dp.materialized_view(name="silver_dim_sku_catalog")
def silver_dim_sku_catalog():
    return spark.read.table("dim_sku_catalog").dropDuplicates(["sku_id"])


@dp.materialized_view(name="silver_dim_retail_customers")
def silver_dim_retail_customers():
    return spark.read.table("dim_retail_customers").dropDuplicates(["customer_id"])


@dp.materialized_view(name="fact_production_batches")
def fact_production_batches():
    return (
        spark.read.table("historical_production_batches")
        .withColumn("planned_qty_units", col("planned_qty_units").cast("int"))
        .withColumn("actual_qty_units", col("actual_qty_units").cast("int"))
        .withColumn("defect_units", col("defect_units").cast("int"))
        .withColumn("yield_pct", spark_round(
            (col("actual_qty_units") - col("defect_units")) / col("actual_qty_units") * 100, 2))
        .dropDuplicates(["batch_id"])
    )


@dp.materialized_view(name="fact_shipments")
def fact_shipments():
    return (
        spark.read.table("historical_shipments")
        .withColumn("qty_units", col("qty_units").cast("int"))
        .dropDuplicates(["shipment_id"])
    )


@dp.materialized_view(name="fact_quality_audits")
def fact_quality_audits():
    return spark.read.table("historical_quality_audit_logs").dropDuplicates(["audit_id"])


# --- fact_sensor_readings: streaming target, union of live + batch, keyed on reading_id ---
SENSOR_COLS = [
    "reading_id", "line_id", "event_timestamp", "temperature_c",
    "pressure_bar", "fill_volume_ml", "vibration_mm_s",
    "machine_status", "anomaly_flag", "data_source"
]


def _prep(df, source_tag):
    return (
        df.withColumn("event_timestamp", col(
            "event_timestamp").cast("timestamp"))
          .withColumn("temperature_c", col("temperature_c").cast("double"))
          .withColumn("pressure_bar", col("pressure_bar").cast("double"))
          .withColumn("fill_volume_ml", col("fill_volume_ml").cast("double"))
          .withColumn("vibration_mm_s", col("vibration_mm_s").cast("double"))
          .withColumn("anomaly_flag", col("anomaly_flag").cast("int"))
          .withColumn("data_source", lit(source_tag))
          .select(*SENSOR_COLS)
    )


dp.create_streaming_table(name="fact_sensor_readings")

dp.create_auto_cdc_flow(
    target="fact_sensor_readings",
    source="sensor_readings_stream",
    keys=["reading_id"],
    sequence_by=col("event_timestamp"),
    stored_as_scd_type=1
)


@dp.append_flow(target="fact_sensor_readings")
def batch_sensor_flow():
    return _prep(spark.read.table("sensor_readings"), "batch_file")
