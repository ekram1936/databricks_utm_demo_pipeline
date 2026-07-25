"""
LLM-based sentiment/root-cause analysis on quality audit notes.
Uses Databricks ai_query() with a foundation model endpoint (e.g. Llama).

Run this from a Databricks notebook or job:
    from src.pipeline import sentiment_analysis
    sentiment_analysis.run(spark)
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import azure_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


def run(spark):
    silver = azure_settings.SILVER_SCHEMA
    gold = azure_settings.GOLD_SCHEMA

    query = f"""
    CREATE OR REPLACE TABLE {gold}.d_audit_sentiment AS
    SELECT
      audit_id,
      batch_id,
      audit_outcome,
      audit_notes,
      ai_query(
        '{MODEL_ENDPOINT}',
        CONCAT('Classify the sentiment of this quality audit note as Positive, Neutral, or Negative, and give a one-word root cause category if negative. Note: ', audit_notes)
      ) AS llm_analysis
    FROM {silver}.fact_quality_audits
    """
    logger.info(f"Running LLM sentiment analysis via {MODEL_ENDPOINT} ...")
    spark.sql(query)
    logger.info(f"Wrote results to {gold}.d_audit_sentiment")
