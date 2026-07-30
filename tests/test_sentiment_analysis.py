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


class TestSentimentIdempotentFlow:
    def test_run_creates_table_if_missing_and_then_inserts_new_rows(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        run(mock_spark)
        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert len(calls) == 2
        assert "CREATE TABLE IF NOT EXISTS" in calls[0]
        assert "INSERT INTO" in calls[1]
        assert "LEFT ANTI JOIN" in calls[1]

    def test_create_query_defines_expected_schema(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        run(mock_spark)
        create_query = mock_spark.sql.call_args_list[0].args[0]
        for col in [
            "audit_id STRING",
            "batch_id STRING",
            "audit_outcome STRING",
            "audit_notes STRING",
            "llm_analysis STRING",
        ]:
            assert col in create_query

    def test_insert_query_uses_audit_id_deduplication(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        run(mock_spark)
        insert_query = mock_spark.sql.call_args_list[1].args[0]
        assert "LEFT ANTI JOIN" in insert_query
        assert "s.audit_id = g.audit_id" in insert_query


class TestSentimentQueryContent:
    def test_query_references_correct_model_endpoint(self):
        from src.pipeline.sentiment_analysis import run, MODEL_ENDPOINT
        mock_spark = MagicMock()
        run(mock_spark)
        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert any(MODEL_ENDPOINT in q for q in calls)

    def test_query_targets_gold_audit_sentiment_table(self):
        from src.pipeline.sentiment_analysis import run
        from config import azure_settings
        mock_spark = MagicMock()
        run(mock_spark)
        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert any(azure_settings.GOLD_SCHEMA + ".d_audit_sentiment" in q for q in calls)

    def test_query_reads_from_silver_quality_audits(self):
        from src.pipeline.sentiment_analysis import run
        from config import azure_settings
        mock_spark = MagicMock()
        run(mock_spark)
        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert any(azure_settings.SILVER_SCHEMA + ".fact_quality_audits" in q for q in calls)

    def test_query_uses_ai_query_function(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        run(mock_spark)
        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert any("ai_query(" in q for q in calls)

    def test_query_selects_required_audit_columns(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()
        run(mock_spark)
        combined = " ".join(c.args[0] for c in mock_spark.sql.call_args_list)
        for col in ["audit_id", "batch_id", "audit_outcome", "audit_notes"]:
            assert col in combined


class TestSentimentRunResilience:
    def test_run_raises_if_create_step_fails(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()

        def side_effect(query):
            if query.startswith("CREATE TABLE IF NOT EXISTS"):
                raise Exception("Create failed")
            return MagicMock()

        mock_spark.sql.side_effect = side_effect

        with pytest.raises(Exception, match="Create failed"):
            run(mock_spark)

    def test_run_raises_if_insert_step_fails(self):
        from src.pipeline.sentiment_analysis import run
        mock_spark = MagicMock()

        def side_effect(query):
            if query.startswith("INSERT INTO"):
                raise Exception("Insert failed")
            return MagicMock()

        mock_spark.sql.side_effect = side_effect

        with pytest.raises(Exception, match="Insert failed"):
            run(mock_spark)


class TestSentimentMainEntrypoint:
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