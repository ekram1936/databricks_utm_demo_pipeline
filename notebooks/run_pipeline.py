# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys
sys.path.append("/Workspace/Repos/databricks_utm_demo_pipeline")  # adjust to your repo path

from src.pipeline import bronze_ingest, silver_transform, gold_aggregate, sentiment_analysis
from src.utils.logger import get_logger

logger = get_logger("run_pipeline")

# COMMAND ----------

logger.info("STEP 1/4: Bronze ingestion")
bronze_ingest.run(spark)

# COMMAND ----------

logger.info("STEP 2/4: Silver transformation")
silver_transform.run(spark)

# COMMAND ----------

logger.info("STEP 3/4: Gold aggregation")
gold_aggregate.run(spark)

# COMMAND ----------

logger.info("STEP 4/4: LLM sentiment analysis on quality audits")
sentiment_analysis.run(spark)

# COMMAND ----------

logger.info("PIPELINE COMPLETE: Bronze -> Silver -> Gold -> Sentiment")
