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


def run(spark):
    silver = azure_settings.SILVER_SCHEMA
    gold = azure_settings.GOLD_SCHEMA
    target_table = gold + ".d_audit_sentiment"
    source_table = silver + ".fact_quality_audits"

    create_query = (
        "CREATE TABLE IF NOT EXISTS " + target_table + " ("
        "audit_id STRING, "
        "batch_id STRING, "
        "audit_outcome STRING, "
        "audit_notes STRING, "
        "llm_analysis STRING"
        ")"
    )

    logger.info("Ensuring " + target_table + " exists ...")
    spark.sql(create_query)

    insert_query = (
        "INSERT INTO " + target_table + " SELECT "
        "s.audit_id, s.batch_id, s.audit_outcome, s.audit_notes, " + _ai_query_expr("s.audit_notes") +
        " FROM " + source_table + " s "
        "LEFT ANTI JOIN " + target_table + " g "
        "ON s.audit_id = g.audit_id"
    )

    logger.info("Incrementally scoring new audit_ids via " + MODEL_ENDPOINT + " ...")
    spark.sql(insert_query)
    logger.info("Sentiment analysis complete for " + target_table)


def main():
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()