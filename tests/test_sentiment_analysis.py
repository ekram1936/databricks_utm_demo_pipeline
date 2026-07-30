"""
Sentiment/root-cause analysis layer tests (LLM-based quality audit analysis).
run() branches on whether the Gold table already exists:
  - First run: CREATE TABLE ... scores ALL audit records.
  - Subsequent runs: INSERT INTO ... via LEFT ANTI JOIN, scoring only NEW audit_ids.
ai_query() requires a live Databricks model endpoint, so we mock spark itself
(spark.catalog.tableExists + spark.sql) and assert on the generated SQL string,
rather than mocking a standalone Python function (none exists - the LLM call
is inline SQL).
"""
import pytest
from unittest.mock import MagicMock, patch


class TestSentimentFirstRunCreatesTable:
    """Validates the CREATE TABLE branch runs when the Gold table doesn't exist yet."""

    def test_create_table_used_when_table_missing(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE" in executed_query
        assert "INSERT INTO" not in executed_query

    def test_first_run_scores_all_rows_no_anti_join(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert "LEFT ANTI JOIN" not in executed_query

    def test_spark_sql_called_exactly_once_on_first_run(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        run(mock_spark)
        mock_spark.sql.assert_called_once()


class TestSentimentIncrementalRunSkipsExisting:
    """Validates the INSERT + LEFT ANTI JOIN branch runs on subsequent runs."""

    def test_insert_into_used_when_table_exists(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert "INSERT INTO" in executed_query
        assert "CREATE TABLE" not in executed_query

    def test_incremental_run_uses_left_anti_join_on_audit_id(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert "LEFT ANTI JOIN" in executed_query
        assert "s.audit_id = g.audit_id" in executed_query

    def test_incremental_run_checks_table_exists_before_deciding(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        run(mock_spark)
        mock_spark.catalog.tableExists.assert_called_once()


class TestSentimentQueryContent:
    """Validates the generated SQL references correct schemas, tables, columns, model."""

    @pytest.mark.parametrize("table_exists", [True, False])
    def test_query_references_correct_model_endpoint(self, table_exists):
        from src.pipeline.sentiment_analysis import run, MODEL_ENDPOINT
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = table_exists
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert MODEL_ENDPOINT in executed_query

    @pytest.mark.parametrize("table_exists", [True, False])
    def test_query_targets_gold_audit_sentiment_table(self, table_exists):
        from src.pipeline.sentiment_analysis import run
        from config import azure_settings
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = table_exists
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert f"{azure_settings.GOLD_SCHEMA}.d_audit_sentiment" in executed_query

    @pytest.mark.parametrize("table_exists", [True, False])
    def test_query_reads_from_silver_quality_audits(self, table_exists):
        from src.pipeline.sentiment_analysis import run
        from config import azure_settings
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = table_exists
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert f"{azure_settings.SILVER_SCHEMA}.fact_quality_audits" in executed_query

    @pytest.mark.parametrize("table_exists", [True, False])
    def test_query_uses_ai_query_function(self, table_exists):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = table_exists
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        assert "ai_query(" in executed_query

    @pytest.mark.parametrize("table_exists", [True, False])
    def test_query_selects_required_audit_columns(self, table_exists):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = table_exists
        run(mock_spark)
        executed_query = mock_spark.sql.call_args[0][0]
        for col in ["audit_id", "batch_id", "audit_outcome", "audit_notes"]:
            assert col in executed_query


class TestSentimentRunResilience:
    """Confirms run() propagates spark.sql errors rather than silently swallowing them."""

    def test_run_raises_if_spark_sql_fails(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_spark.sql.side_effect = Exception("Model endpoint unavailable")
        with pytest.raises(Exception, match="Model endpoint unavailable"):
            run(mock_spark)

    def test_run_raises_if_table_exists_check_fails(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.side_effect = Exception("Catalog unreachable")
        with pytest.raises(Exception, match="Catalog unreachable"):
            run(mock_spark)


class TestSentimentMainEntrypoint:
    """Validates main() correctly obtains or creates a SparkSession before calling run()."""

    @patch("src.pipeline.sentiment_analysis.run")
    @patch("src.pipeline.sentiment_analysis.SparkSession")
    def test_main_uses_active_session_if_available(self, mock_session_cls, mock_run):
        mock_active = MagicMock()
        mock_session_cls.getActiveSession.return_value = mock_active
        from src.pipeline.sentiment_analysis import main
        main()
        mock_run.assert_called_once_with(mock_active)

    @patch("src.pipeline.sentiment_analysis.run")
    @patch("src.pipeline.sentiment_analysis.SparkSession")
    def test_main_creates_new_session_if_none_active(self, mock_session_cls, mock_run):
        mock_session_cls.getActiveSession.return_value = None
        mock_new_session = MagicMock()
        mock_session_cls.builder.getOrCreate.return_value = mock_new_session
        from src.pipeline.sentiment_analysis import main
        main()
        mock_run.assert_called_once_with(mock_new_session)