"""
Azure sync (upload_to_adls.py) tests: local-to-cloud upload logic, mocked Azure SDK.
No real Azure credentials or network calls happen in CI.
"""
import os
import pytest
from unittest.mock import patch, MagicMock, mock_open


class TestUploadFolderLogic:
    """Validates the local folder -> ADLS directory mapping and skip-if-missing behavior."""

    def test_missing_local_folder_returns_zero_and_does_not_crash(self, tmp_path):
        from src.azure_sync.upload_to_adls import upload_folder
        fake_file_system = MagicMock()
        missing_dir = str(tmp_path / "does_not_exist")
        result = upload_folder(missing_dir, "dim", fake_file_system)
        assert result == 0

    def test_upload_folder_counts_only_files_not_subdirectories(self, tmp_path):
        from src.azure_sync.upload_to_adls import upload_folder
        (tmp_path / "file1.csv").write_text("a,b\n1,2")
        (tmp_path / "file2.csv").write_text("c,d\n3,4")
        (tmp_path / "subdir").mkdir()

        fake_file_system = MagicMock()
        fake_directory_client = MagicMock()
        fake_file_system.get_directory_client.return_value = fake_directory_client
        fake_file_client = MagicMock()
        fake_directory_client.get_file_client.return_value = fake_file_client

        result = upload_folder(str(tmp_path), "dim", fake_file_system)
        assert result == 2  # subdir should not be counted

    def test_upload_uses_overwrite_true(self, tmp_path):
        from src.azure_sync.upload_to_adls import upload_folder
        (tmp_path / "file1.csv").write_text("a,b\n1,2")

        fake_file_system = MagicMock()
        fake_directory_client = MagicMock()
        fake_file_system.get_directory_client.return_value = fake_directory_client
        fake_file_client = MagicMock()
        fake_directory_client.get_file_client.return_value = fake_file_client

        upload_folder(str(tmp_path), "dim", fake_file_system)
        _, kwargs = fake_file_client.upload_data.call_args
        assert kwargs.get("overwrite") is True


class TestAuthenticationFallback:
    """Confirms connection-string auth is tried before falling back to DefaultAzureCredential."""

    @patch.dict(os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "fake-conn-str"})
    @patch("azure.storage.filedatalake.DataLakeServiceClient.from_connection_string")
    def test_uses_connection_string_when_env_var_present(self, mock_from_conn_str):
        from src.azure_sync.upload_to_adls import get_service_client
        get_service_client()
        mock_from_conn_str.assert_called_once_with("fake-conn-str")

    @patch.dict(os.environ, {}, clear=True)
    @patch("azure.identity.DefaultAzureCredential")
    @patch("azure.storage.filedatalake.DataLakeServiceClient.__init__", return_value=None)
    def test_falls_back_to_default_credential_when_no_conn_str(self, mock_init, mock_cred):
        from src.azure_sync.upload_to_adls import get_service_client
        get_service_client()
        mock_cred.assert_called_once()


class TestRunOrchestration:
    """Validates the mapping between local raw folders and remote ADLS container paths."""

    @patch("src.azure_sync.upload_to_adls.upload_folder")
    @patch("src.azure_sync.upload_to_adls.get_service_client")
    def test_run_uploads_all_three_mapped_folders(self, mock_get_client, mock_upload):
        mock_upload.return_value = 3
        mock_service_client = MagicMock()
        mock_get_client.return_value = mock_service_client
        from src.azure_sync.upload_to_adls import run
        total = run()
        assert total == 9  # 3 folders x 3 files each (mocked)
        assert mock_upload.call_count == 3
