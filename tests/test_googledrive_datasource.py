"""
Unit tests for GoogleDriveDatasourceConnector.

These tests use hardcoded credentials and make real API calls to Google Drive.
Set up your Google Drive OAuth credentials in the setUp method before running tests.
"""

import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from livedocs.datasources.googledrive import GoogleDriveDatasourceConnector
from livedocs.types import (
    FileConnectorType,
    FileNodeType,
    GoogleDriveConnectorInfo,
    MountHealthStatus,
)


class TestGoogleDriveDatasourceConnector(unittest.TestCase):
    """Test suite for GoogleDriveDatasourceConnector."""

    def setUp(self):
        """Set up test fixtures with hardcoded credentials."""
        # Hardcoded Google Drive OAuth credentials - REPLACE WITH YOUR TEST CREDENTIALS
        self.connector_id = str(uuid.uuid4())
        self.connector_info: GoogleDriveConnectorInfo = {
            "connector_id": self.connector_id,
            "name": "Test Google Drive Connector",
            "provider": "google",
            "email": "arsalan.b4@gmail.com",  # Replace with your test email
            "access_token": "<YOUR_ACCESS_TOKEN>",  # Replace with your test access token
            "refresh_token": "<YOUR_REFRESH_TOKEN>",  # Replace with your test refresh token
            "token_expiry": datetime(
                2025, 12, 6, 18, 11, 15, 122967, tzinfo=timezone.utc
            ),
            "scopes": "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/userinfo.email openid",  # Replace with your scopes
        }

        # Create connector instance
        self.connector = GoogleDriveDatasourceConnector()

        # Create get_connection_details callable
        def get_connection_details(conn_id: str) -> tuple[object, dict[str, Any]]:
            if conn_id == self.connector_id:
                return (None, self.connector_info)
            raise KeyError(f"Connector {conn_id} not found")

        self.get_connection_details = get_connection_details

        # Create no-op refresh token callback for tests
        def refresh_token_callback(
            connector_info: GoogleDriveConnectorInfo,
        ) -> GoogleDriveConnectorInfo:
            # No-op: just return the same connector info
            return connector_info

        self.refresh_token_callback = refresh_token_callback

        # Test file/folder paths
        self.test_file_name = "test_file.txt"
        self.test_file_path = self.test_file_name
        self.test_file_path_nested = "test_folder/test_file_nested.txt"
        self.test_folder_name = "test_folder"
        self.test_folder_path = self.test_folder_name

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up any temporary files
        pass

    # Static Helper Method Tests

    def test_create_google_drive_service(self):
        """Test Google Drive service creation with OAuth tokens."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            self.assertIsNotNone(service)
        except Exception as e:
            # If credentials are invalid, fail with a clear message
            self.fail(
                f"Could not create Google Drive service. "
                + f"Check your OAuth credentials (access_token, refresh_token, token_expiry). "
                + f"Error: {e}"
            )

    def test_generate_file_id(self):
        """Test deterministic UUID generation from connector_id and path."""
        path1 = "test/path/file.txt"
        path2 = "test/path/file2.txt"

        # Same path should generate same UUID
        id1 = GoogleDriveDatasourceConnector._generate_file_id(self.connector_id, path1)
        id2 = GoogleDriveDatasourceConnector._generate_file_id(self.connector_id, path1)
        self.assertEqual(id1, id2)

        # Different paths should generate different UUIDs
        id3 = GoogleDriveDatasourceConnector._generate_file_id(self.connector_id, path2)
        self.assertNotEqual(id1, id3)

        # Different connector IDs should generate different UUIDs for same path
        other_connector_id = str(uuid.uuid4())
        id4 = GoogleDriveDatasourceConnector._generate_file_id(
            other_connector_id, path1
        )
        self.assertNotEqual(id1, id4)

    def test_get_parent_path(self):
        """Test parent path extraction."""
        # Root path
        self.assertIsNone(GoogleDriveDatasourceConnector._get_parent_path(""))
        self.assertIsNone(GoogleDriveDatasourceConnector._get_parent_path("/"))
        self.assertIsNone(GoogleDriveDatasourceConnector._get_parent_path("file.txt"))

        # Nested paths
        self.assertEqual(
            GoogleDriveDatasourceConnector._get_parent_path("folder/file.txt"),
            "folder",
        )
        self.assertEqual(
            GoogleDriveDatasourceConnector._get_parent_path("folder1/folder2/file.txt"),
            "folder1/folder2",
        )

        # Paths with trailing slashes
        # "folder/" after stripping becomes "folder", which has no parent (it's at root)
        self.assertIsNone(GoogleDriveDatasourceConnector._get_parent_path("folder/"))

    # Instance Helper Method Tests

    def test_get_folder_id_from_path_root(self):
        """Test getting folder ID for root folder."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            folder_id = self.connector._get_folder_id_from_path(service, None)
            self.assertEqual(folder_id, "root")

            folder_id = self.connector._get_folder_id_from_path(service, "")
            self.assertEqual(folder_id, "root")

            folder_id = self.connector._get_folder_id_from_path(service, "/")
            self.assertEqual(folder_id, "root")
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_folder_id_from_path_nested(self):
        """Test getting folder ID for nested folder path."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            # Try to get folder ID for a nested path
            # Note: This will return "root" if folder doesn't exist
            folder_id = self.connector._get_folder_id_from_path(
                service, self.test_folder_path
            )
            self.assertIsInstance(folder_id, str)
            # Should return "root" if folder doesn't exist, or actual folder ID if it does
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_folder_id_from_path_nonexistent(self):
        """Test getting folder ID for non-existent folder returns root."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            folder_id = self.connector._get_folder_id_from_path(
                service, "nonexistent_folder_xyz123"
            )
            # Should return "root" as fallback
            self.assertEqual(folder_id, "root")
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_file_id_from_path_root(self):
        """Test getting file ID for file in root."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            # Try to get file ID (will return None if file doesn't exist)
            file_id = self.connector._get_file_id_from_path(
                service, self.test_file_path
            )
            # Should be None if file doesn't exist, or a string ID if it does
            if file_id is not None:
                self.assertIsInstance(file_id, str)
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_file_id_from_path_nested(self):
        """Test getting file ID for file in nested folder."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            file_id = self.connector._get_file_id_from_path(
                service, self.test_file_path_nested
            )
            if file_id is not None:
                self.assertIsInstance(file_id, str)
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_file_id_from_path_nonexistent(self):
        """Test getting file ID for non-existent file returns None."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            file_id = self.connector._get_file_id_from_path(
                service, "nonexistent_file_xyz456.txt"
            )
            self.assertIsNone(file_id)
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    def test_get_file_id_from_path_empty(self):
        """Test getting file ID for empty path returns None."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            file_id = self.connector._get_file_id_from_path(service, "")
            self.assertIsNone(file_id)

            # Note: _get_file_id_from_path doesn't accept None, so we skip that test
        except Exception as e:
            self.fail(f"Could not create Google Drive service: {e}")

    # Public Method Tests

    def test_list_root(self):
        """Test listing files/directories from root."""
        nodes = self.connector.list(
            path="",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        self.assertIsInstance(nodes, list)
        # Verify FileNode structure for any returned nodes
        for node in nodes:
            self.assertIsInstance(node.id, UUID)
            self.assertIsInstance(node.name, str)
            self.assertIn(node.type, [FileNodeType.file, FileNodeType.directory])
            self.assertEqual(node.mount_type, FileConnectorType.googledrive)
            self.assertEqual(node.connector_id, UUID(self.connector_id))
            self.assertIsInstance(node.path, str)
            self.assertIsNotNone(node.health)
            self.assertEqual(node.health["status"], MountHealthStatus.connected)

    def test_list_nested_folder(self):
        """Test listing files/directories from nested folder."""
        nodes = self.connector.list(
            path=self.test_folder_path,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        self.assertIsInstance(nodes, list)
        for node in nodes:
            self.assertIsInstance(node.id, UUID)
            self.assertIsInstance(node.path, str)

    def test_list_file_node_structure(self):
        """Test FileNode structure in list results."""
        nodes = self.connector.list(
            path="",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        for node in nodes:
            # Check all required fields
            self.assertIsNotNone(node.id)
            self.assertIsNotNone(node.name)
            self.assertIsNotNone(node.type)
            self.assertIsNotNone(node.mount_type)
            self.assertIsNotNone(node.connector_id)
            self.assertIsNotNone(node.path)
            self.assertIsNotNone(node.health)

            # Check type-specific fields
            if node.type == FileNodeType.file:
                # Files can have size and mime_type
                if node.size is not None:
                    self.assertIsInstance(node.size, int)
                if node.mime_type is not None:
                    self.assertIsInstance(node.mime_type, str)
            elif node.type == FileNodeType.directory:
                # Directories should not have size or mime_type
                self.assertIsNone(node.size)
                self.assertIsNone(node.mime_type)

    def test_list_date_parsing(self):
        """Test date parsing in list results."""
        nodes = self.connector.list(
            path="",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        for node in nodes:
            # Dates can be None or datetime objects
            if node.modified_at is not None:
                self.assertIsInstance(node.modified_at, datetime)
            if node.created_at is not None:
                self.assertIsInstance(node.created_at, datetime)

    def test_list_none_parameters(self):
        """Test list() with None parameters returns empty list."""
        nodes = self.connector.list(
            path=None,
            connector_id=None,
            get_connection_details=None,
        )
        self.assertEqual(nodes, [])

        nodes = self.connector.list(
            path="test",
            connector_id=None,
            get_connection_details=None,
        )
        self.assertEqual(nodes, [])

    def test_list_invalid_connector_id(self):
        """Test list() with invalid connector_id returns empty list."""
        nodes = self.connector.list(
            path="test",
            connector_id="invalid-id",
            get_connection_details=self.get_connection_details,
        )
        self.assertEqual(nodes, [])

    def test_delete_file_existing(self):
        """Test deleting an existing file."""
        # First upload a file
        test_content = b"File to delete"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name

        try:
            # Upload file
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=None,  # Upload to root
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.fail("Could not upload test file - check credentials")

            # Get the file path (use basename)
            file_name = os.path.basename(temp_file_path)

            # Delete file
            result = self.connector.delete_file(
                file_path=file_name,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            self.assertTrue(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_delete_file_nonexistent(self):
        """Test deleting a non-existent file returns False."""
        result = self.connector.delete_file(
            file_path="nonexistent_file_xyz789.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_delete_file_wrong_connector_type(self):
        """Test delete_file() with wrong connector_type returns False."""
        result = self.connector.delete_file(
            file_path="test.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_rename_file(self):
        """Test renaming a file."""
        # Upload a file first
        test_content = b"File to rename"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name
        original_name = os.path.basename(temp_file_path)

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=None,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.fail("Could not upload test file - check credentials")

            # Rename file
            new_name = "renamed_file.txt"
            result = self.connector.rename_file(
                file_path=original_name,
                new_name=new_name,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            self.assertTrue(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_rename_file_nonexistent(self):
        """Test renaming a non-existent file returns False."""
        result = self.connector.rename_file(
            file_path="nonexistent_file_xyz999.txt",
            new_name="new_name.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_rename_file_wrong_connector_type(self):
        """Test rename_file() with wrong connector_type returns False."""
        result = self.connector.rename_file(
            file_path="test.txt",
            new_name="new_name.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_save_file_to_root(self):
        """Test uploading a file to root."""
        test_content = b"Test upload content to root"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name

        try:
            result = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=None,  # Upload to root
                get_connection_details=self.get_connection_details,
            )

            if not result:
                self.fail("Could not upload file - check credentials")

            self.assertTrue(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_save_file_to_specific_folder(self):
        """Test uploading a file to specific folder."""
        test_content = b"Test upload content to folder"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name

        try:
            result = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=self.test_folder_path,
                get_connection_details=self.get_connection_details,
            )

            if not result:
                self.fail("Could not upload file - check credentials")

            self.assertTrue(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_save_file_nonexistent_local(self):
        """Test uploading a non-existent local file returns False."""
        result = self.connector.save_file(
            file_path="/nonexistent/path/file.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_save_file_wrong_connector_type(self):
        """Test save_file() with wrong connector_type returns False."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file_path = f.name

        try:
            result = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertFalse(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_get_signed_url_existing(self):
        """Test getting download URL for existing file."""
        # First upload a file
        test_content = b"File for signed URL"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name
        file_name = os.path.basename(temp_file_path)

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=None,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.fail("Could not upload test file - check credentials")

            # Get signed URL
            url = self.connector.get_signed_url(
                file_path=file_name,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                expiration_seconds=3600,  # Ignored for Google Drive
                get_connection_details=self.get_connection_details,
            )

            self.assertIsNotNone(url)
            self.assertIsInstance(url, str)
            if url is not None:
                self.assertTrue(url.startswith("http"))
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_get_signed_url_nonexistent(self):
        """Test getting download URL for non-existent file returns None."""
        url = self.connector.get_signed_url(
            file_path="nonexistent_file_xyz111.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(url)

    def test_get_signed_url_wrong_connector_type(self):
        """Test get_signed_url() with wrong connector_type returns None."""
        url = self.connector.get_signed_url(
            file_path="test.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(url)

    def test_get_signed_url_expiration_ignored(self):
        """Test that expiration_seconds parameter is ignored for Google Drive."""
        # First upload a file
        test_content = b"File for expiration test"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            f.flush()
            temp_file_path = f.name
        file_name = os.path.basename(temp_file_path)

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                drive_path=None,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.fail("Could not upload test file - check credentials")

            # Get URL with different expiration values (should both work)
            url1 = self.connector.get_signed_url(
                file_path=file_name,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                expiration_seconds=3600,
                get_connection_details=self.get_connection_details,
            )

            url2 = self.connector.get_signed_url(
                file_path=file_name,
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                expiration_seconds=7200,
                get_connection_details=self.get_connection_details,
            )

            # Both should return URLs (expiration is ignored)
            self.assertIsNotNone(url1)
            self.assertIsNotNone(url2)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_teardown(self):
        """Test teardown() is a no-op."""
        # Should not raise any exceptions
        self.connector.teardown()

    # Edge Cases & Error Handling

    def test_list_error_handling(self):
        """Test error handling in list() method."""
        # Test with invalid credentials
        invalid_info = self.connector_info.copy()
        invalid_info["access_token"] = "invalid_token"

        def get_invalid_connection_details(
            conn_id: str,
        ) -> tuple[object, dict[str, Any]]:
            if conn_id == self.connector_id:
                return (None, invalid_info)
            raise KeyError(f"Connector {conn_id} not found")

        nodes = self.connector.list(
            path="",
            connector_id=self.connector_id,
            get_connection_details=get_invalid_connection_details,
        )

        # Should return empty list or nodes with error health status
        self.assertIsInstance(nodes, list)
        if nodes:
            for node in nodes:
                self.assertEqual(node.health["status"], MountHealthStatus.error)

    def test_folder_name_escaping(self):
        """Test folder/file name escaping (single quotes)."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            # Test with folder name containing single quote
            folder_name_with_quote = "test'folder"
            folder_id = self.connector._get_folder_id_from_path(
                service, folder_name_with_quote
            )
            # Should not raise exception due to quote escaping
            self.assertIsInstance(folder_id, str)
        except Exception as e:
            self.fail(f"Could not test folder name escaping: {e}")

    def test_file_name_escaping(self):
        """Test file name escaping (single quotes)."""
        try:
            service = GoogleDriveDatasourceConnector._create_google_drive_service(
                self.connector_info
            )
            # Test with file name containing single quote
            file_name_with_quote = "test'file.txt"
            file_id = self.connector._get_file_id_from_path(
                service, file_name_with_quote
            )
            # Should not raise exception due to quote escaping
            if file_id is not None:
                self.assertIsInstance(file_id, str)
        except Exception as e:
            self.fail(f"Could not test file name escaping: {e}")


if __name__ == "__main__":
    unittest.main()
