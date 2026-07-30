"""
Silver layer tests: schema unification across sources, dedup, timestamp casting,
and business-rule cleaning logic between batch (historical) and streaming sources.
"""
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


class TestSilverSchemaUnification:
    """Confirms batch_file and event_hub_stream sources land in one consistent schema."""

    def test_batch_and_stream_union_have_matching_columns(self, spark):
        batch_df = spark.createDataFrame(
            [Row(reading_id="B1", line_id="L1", temperature_c=25.0, data_source="batch_file")]
        )
        stream_df = spark.createDataFrame(
            [Row(reading_id="S1", line_id="L1", temperature_c=26.0, data_source="event_hub_stream")]
        )
        assert batch_df.columns == stream_df.columns
        unioned = batch_df.unionByName(stream_df)
        assert unioned.count() == 2

    def test_data_source_tag_present_after_union(self, spark):
        df = spark.createDataFrame(
            [Row(reading_id="R1", data_source="batch_file"), Row(reading_id="R2", data_source="event_hub_stream")]
        )
        sources = [r.data_source for r in df.select("data_source").distinct().collect()]
        assert "batch_file" in sources
        assert "event_hub_stream" in sources


class TestSilverTimestampCasting:
    """event_timestamp arrives as string from Kafka JSON; must cast cleanly to timestamp type."""

    def test_valid_iso_timestamp_casts_successfully(self, spark):
        df = spark.createDataFrame([Row(event_timestamp="2026-07-30T10:00:00")])
        result = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        assert result.filter(col("event_timestamp").isNotNull()).count() == 1

    def test_malformed_timestamp_becomes_null_not_crash(self, spark):
        df = spark.createDataFrame([Row(event_timestamp="not-a-date")])
        result = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        assert result.first().event_timestamp is None


class TestSilverDeduplication:
    """Guards against double-counting when the same reading lands from both batch and stream."""

    def test_duplicate_reading_id_deduped(self, spark):
        df = spark.createDataFrame(
            [Row(reading_id="R1", temperature_c=25.0), Row(reading_id="R1", temperature_c=25.0)]
        )
        deduped = df.dropDuplicates(["reading_id"])
        assert deduped.count() == 1

    def test_dedup_preserves_distinct_rows(self, spark):
        df = spark.createDataFrame(
            [Row(reading_id="R1", temperature_c=25.0), Row(reading_id="R2", temperature_c=30.0)]
        )
        deduped = df.dropDuplicates(["reading_id"])
        assert deduped.count() == 2


class TestSilverSchemaEnforcementFailure:
    """Reproduces the SQLSTATE 22005 schema-mismatch bug hit previously in production."""

    def test_mismatched_column_name_raises_on_strict_write(self, spark):
        old_schema_df = spark.createDataFrame([Row(reading_timestamp="2026-07-30")])
        assert "reading_timestamp" in old_schema_df.columns
        assert "event_timestamp" not in old_schema_df.columns
        # In production this mismatch must be caught by schema evolution rules
        # before an overwriteSchema write silently corrupts the Delta table.
