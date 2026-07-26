"""
config/azure_settings.py
Loads Azure / Databricks configuration from environment variables, sourced from
a local .env file (via python-dotenv) or real environment variables in CI/CD
and Databricks job configs.

Requires: pip install python-dotenv
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Locate and load .env from project root, regardless of where this module is imported from
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# --- Azure Event Hubs ---
EVENT_HUB_CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "sensor-telemetry")
EVENT_HUB_NAMESPACE = os.environ.get(
    "EVENT_HUB_NAMESPACE", "utmdemo-eventhub-ns")

# --- ADLS / Storage (for local SDK-based sync, if still used) ---
AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", "")
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME", "")
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "raw")

# --- Databricks Unity Catalog ---
CATALOG = os.environ.get("DATABRICKS_CATALOG", "utm_demo_catalog")
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# --- Databricks workspace (for CLI/SDK sync jobs) ---
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
