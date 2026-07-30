import sys
import os

_current = os.path.dirname(os.path.abspath(__file__))
for _ in range(6):
    if os.path.isdir(os.path.join(_current, "config")):
        if _current not in sys.path:
            sys.path.insert(0, _current)
        break
    _current = os.path.dirname(_current)

"""
Uploads local raw data (data/raw/) to Azure Data Lake Storage Gen2.
Runs LOCALLY (not on Databricks) -- Databricks later reads this same data
via the Unity Catalog External Location already configured against this
storage account, so no dbutils/pyspark needed here.

Requires: pip install azure-storage-file-datalake azure-identity
Auth: uses DefaultAzureCredential, which automatically falls back to your
`az login` session locally. Just run `az login` once before executing this.
"""
from src.utils.logger import get_logger
from config import settings, azure_settings
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


logger = get_logger(__name__)


def get_service_client():
    from azure.storage.filedatalake import DataLakeServiceClient

    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return DataLakeServiceClient.from_connection_string(conn_str)

    from azure.identity import DefaultAzureCredential
    account_url = f"https://{azure_settings.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=DefaultAzureCredential())


def upload_folder(local_folder: str, remote_subpath: str, file_system):
    if not os.path.isdir(local_folder):
        logger.warning(f"Local folder not found, skipping: {local_folder}")
        return 0

    directory_client = file_system.get_directory_client(remote_subpath)
    try:
        directory_client.create_directory()
    except Exception:
        pass  # already exists

    count = 0
    for fname in os.listdir(local_folder):
        local_path = os.path.join(local_folder, fname)
        if not os.path.isfile(local_path):
            continue
        file_client = directory_client.get_file_client(fname)
        with open(local_path, "rb") as f:
            data = f.read()
        file_client.upload_data(data, overwrite=True)
        logger.info(f"Uploaded {fname} -> {remote_subpath}/{fname}")
        count += 1
    return count


def run():
    logger.info("Starting ADLS sync of data/raw/ ...")
    service_client = get_service_client()
    file_system = service_client.get_file_system_client(
        azure_settings.RAW_CONTAINER)

    mapping = {
        settings.RAW_DIM_DIR: "dim",
        settings.RAW_HISTORICAL_DIR: "historical",
        settings.RAW_STREAMING_DIR: "streaming",
    }

    total = 0
    for local_dir, remote_dir in mapping.items():
        total += upload_folder(local_dir, remote_dir, file_system)

    logger.info(f"ADLS sync complete. {total} files uploaded.")
    return total


if __name__ == "__main__":
    run()