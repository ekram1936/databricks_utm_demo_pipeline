import sys
from pyspark.sql import SparkSession

PROJECT_ROOT = "/Workspace/Users/md-ekram.hossain@stud.uni-bamberg.de/.bundle/manufacturing-etl/dev/files"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

PROMPT_TEXT = (
    "Classify the sentiment of this quality audit note as Positive, "
    "Neutral, or Negative, and give a one-word root cause category "
    "if negative. Note: "
)


def _ai_query_expr(note_column):
    return "ai_query('" + MODEL_ENDPOINT + "', CONCAT('" + PROMPT_TEXT + "', " + note_column + ")) AS llm_analysis"


def _table_exists(spark, schema, table):
    try:
        rows = spark.sql("SHOW TABLES IN " + schema + " LIKE '" + table + "'").collect()
        return len(rows) > 0
    except Exception:
        return False


def run(spark):
    silver = azure_settings.SILVER_SCHEMA
    gold = azure_settings.GOLD_SCHEMA
    target_table = gold + ".d_audit_sentiment"
    source_table = silver + ".fact_quality_audits"

    table_exists = _table_exists(spark, gold, "d_audit_sentiment")

    if not table_exists:
        select_clause = "audit_id, batch_id, audit_outcome, audit_notes, " + _ai_query_expr("audit_notes")
        query = "CREATE TABLE " + target_table + " AS SELECT " + select_clause + " FROM " + source_table
        logger.info("Creating " + target_table + " and scoring all audits via " + MODEL_ENDPOINT + " ...")
        spark.sql(query)
        logger.info("Initial load complete for " + target_table)
    else:
        select_clause = "s.audit_id, s.batch_id, s.audit_outcome, s.audit_notes, " + _ai_query_expr("s.audit_notes")
        join_clause = " FROM " + source_table + " s LEFT ANTI JOIN " + target_table + " g ON s.audit_id = g.audit_id"
        query = "INSERT INTO " + target_table + " SELECT " + select_clause + join_clause
        logger.info("Incrementally scoring new audit_ids via " + MODEL_ENDPOINT + " ...")
        spark.sql(query)
        logger.info("Incremental update complete for " + target_table)


def main():
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()
