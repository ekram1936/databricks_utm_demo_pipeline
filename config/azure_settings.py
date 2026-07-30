"""
config/azure_settings.py
Loads Azure / Databricks configuration from environment variables, sourced from
a local .env file (via python-dotenv) when running locally, or real environment
variables in CI/CD and Databricks job configs.

Requires (local dev only): pip install python-dotenv
"""
import os

try:
    from pathlib import Path
    from dotenv import load_dotenv

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    ENV_PATH = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=ENV_PATH)
except Exception:
    pass

# --- Azure Event Hubs ---
EVENT_HUB_CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "sensor-telemetry")
EVENT_HUB_NAMESPACE = os.environ.get(
    "EVENT_HUB_NAMESPACE", "utmdemo-eventhub-ns")

# --- ADLS / Storage ---
AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", "")
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME", "utmdemo26")
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "utmdemo-raw")

# --- Databricks Unity Catalog ---
CATALOG = os.environ.get("DATABRICKS_CATALOG", "utm_demo_catalog")
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# --- Databricks workspace ---
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")


def abfss_path(relative_path: str) -> str:
    """
    Builds an abfss:// path for the raw ADLS container using STORAGE_ACCOUNT_NAME
    and RAW_CONTAINER from this module.

    Example:
        abfss_path("dim/dim_plants.csv")
        -> "abfss://utmdemo-raw@utmdemo26.dfs.core.windows.net/dim/dim_plants.csv"
    """
    container = RAW_CONTAINER or "raw"
    account = STORAGE_ACCOUNT_NAME
    return f"abfss://{container}@{account}.dfs.core.windows.net/{relative_path.lstrip('/')}"