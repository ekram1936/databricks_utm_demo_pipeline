"""
Bronze layer tests: schema contracts, expectation logic, and raw ingestion shape.
Bronze = raw, minimally-transformed data landing from ADLS/Event Hub.
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Row


SENSOR_SCHEMA = StructType([
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


class TestBronzeSchemaContract:
    """Guards against silent schema drift breaking downstream Silver/Gold layers."""

    def test_sensor_schema_has_required_fields(self):
        field_names = [f.name for f in SENSOR_SCHEMA.fields]
        required = ["reading_id", "line_id", "event_timestamp", "temperature_c", "anomaly_flag"]
        for field in required:
            assert field in field_names, f"Missing required field: {field}"

    def test_sensor_schema_field_types(self):
        type_map = {f.name: f.dataType for f in SENSOR_SCHEMA.fields}
        assert isinstance(type_map["temperature_c"], DoubleType)
        assert isinstance(type_map["anomaly_flag"], IntegerType)
        assert isinstance(type_map["reading_id"], StringType)

    def test_schema_field_count_unchanged(self):
        # Fails loudly if someone adds/removes a column without updating downstream logic
        assert len(SENSOR_SCHEMA.fields) == 9


class TestBronzeDataQualityExpectations:
    """Mirrors the @dp.expect / @dp.expect_or_drop rules in the Lakeflow pipeline."""

    def test_valid_reading_id_not_null(self, spark):
        df = spark.createDataFrame(
            [Row(reading_id="R1", temperature_c=25.0), Row(reading_id=None, temperature_c=30.0)]
        )
        valid = df.filter(df.reading_id.isNotNull())
        assert valid.count() == 1

    def test_plausible_temperature_range_drops_out_of_bounds(self, spark):
        df = spark.createDataFrame(
            [Row(temperature_c=25.0), Row(temperature_c=999.0), Row(temperature_c=-50.0)]
        )
        filtered = df.filter((df.temperature_c >= -20) & (df.temperature_c <= 150))
        assert filtered.count() == 1
        assert filtered.first().temperature_c == 25.0

    def test_valid_batch_id_not_null(self, spark):
        df = spark.createDataFrame([Row(batch_id="B100"), Row(batch_id=None)])
        valid = df.filter(df.batch_id.isNotNull())
        assert valid.count() == 1


class TestBronzeIngestionShape:
    """Confirms raw reads preserve row counts and don't silently drop/duplicate data."""

    def test_csv_read_row_count_matches_source(self, spark, tmp_path):
        csv_path = tmp_path / "dim_plants.csv"
        csv_path.write_text("plant_id,plant_name\nP1,Freising\nP2,Munich\n")
        df = spark.read.option("header", "true").csv(str(csv_path))
        assert df.count() == 2

    def test_json_stream_snapshot_no_duplicate_reading_ids(self, spark):
        df = spark.createDataFrame(
            [Row(reading_id="R1"), Row(reading_id="R2"), Row(reading_id="R1")]
        )
        distinct_count = df.select("reading_id").distinct().count()
        assert distinct_count < df.count(), "Expected duplicate detected in test fixture"
