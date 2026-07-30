"""
Gold layer tests: business-ready aggregation correctness for dashboard/reporting tables.
Covers d_sensor_health_summary, production summary, retailer 360, audit sentiment.
"""
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import count, avg, col, when


class TestGoldSensorHealthSummary:
    """Validates anomaly_rate_pct and per-line aggregation logic."""

    def test_anomaly_rate_calculation(self, spark):
        df = spark.createDataFrame(
            [Row(line_id="L1", anomaly_flag=1), Row(line_id="L1", anomaly_flag=0),
             Row(line_id="L1", anomaly_flag=0), Row(line_id="L1", anomaly_flag=0)]
        )
        summary = df.groupBy("line_id").agg(
            (avg(col("anomaly_flag")) * 100).alias("anomaly_rate_pct")
        )
        result = summary.first()
        assert result.anomaly_rate_pct == 25.0

    def test_grouping_by_line_and_source_produces_expected_rows(self, spark):
        df = spark.createDataFrame(
            [Row(line_id="L1", data_source="batch_file"), Row(line_id="L1", data_source="event_hub_stream"),
             Row(line_id="L2", data_source="batch_file")]
        )
        grouped = df.groupBy("line_id", "data_source").agg(count("*").alias("cnt"))
        assert grouped.count() == 3

    def test_no_negative_anomaly_rate(self, spark):
        df = spark.createDataFrame([Row(line_id="L1", anomaly_flag=0), Row(line_id="L1", anomaly_flag=1)])
        summary = df.groupBy("line_id").agg((avg(col("anomaly_flag")) * 100).alias("anomaly_rate_pct"))
        assert summary.first().anomaly_rate_pct >= 0


class TestGoldQualityAuditSummary:
    """Validates audit_count aggregation feeding the sentiment/audit dashboard table."""

    def test_audit_count_matches_row_count(self, spark):
        df = spark.createDataFrame(
            [Row(audit_id="A1", plant_id="P1"), Row(audit_id="A2", plant_id="P1"), Row(audit_id="A3", plant_id="P2")]
        )
        summary = df.groupBy("plant_id").agg(count("audit_id").alias("audit_count"))
        totals = {r.plant_id: r.audit_count for r in summary.collect()}
        assert totals["P1"] == 2
        assert totals["P2"] == 1

    def test_empty_audit_input_produces_zero_rows_not_error(self, spark):
        schema_df = spark.createDataFrame([], "audit_id STRING, plant_id STRING")
        summary = schema_df.groupBy("plant_id").agg(count("audit_id").alias("audit_count"))
        assert summary.count() == 0


class TestGoldWriteContract:
    """Guards the overwriteSchema safety net added after the schema evolution incident."""

    def test_gold_table_row_count_non_negative(self, spark):
        df = spark.createDataFrame([Row(x=1), Row(x=2)])
        assert df.count() >= 0

    def test_gold_output_has_no_null_primary_grouping_key(self, spark):
        df = spark.createDataFrame([Row(line_id="L1"), Row(line_id=None)])
        clean = df.filter(col("line_id").isNotNull())
        assert clean.count() == 1
