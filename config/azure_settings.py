"""
Azure / Databricks connection configuration.
All secrets should be supplied via environment variables, never hardcoded.
"""
import os

# --- Azure Storage (ADLS Gen2) ---
STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "utmdemo26")
RAW_CONTAINER = os.environ.get("AZURE_RAW_CONTAINER", "utmdemo-raw")

def abfss_path(subpath: str = "") -> str:
    """Builds an abfss:// path for the raw container."""
    base = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return f"{base}/{subpath.lstrip('/')}" if subpath else f"{base}/"

# --- Azure Event Hubs ---
EVENT_HUB_CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "sensor-telemetry")
EVENT_HUB_NAMESPACE = os.environ.get("EVENT_HUB_NAMESPACE", "utmdemo-eventhub-ns")

# --- Databricks Unity Catalog ---
CATALOG = os.environ.get("DATABRICKS_CATALOG", "utm_demo_catalog")
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# --- Databricks workspace (for CLI/SDK sync jobs) ---
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
