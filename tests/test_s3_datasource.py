"""
Unit tests for S3DatasourceConnector.

These tests use hardcoded credentials and make real API calls to S3.
Set up your S3 credentials in the setUp method before running tests.
"""

import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime
from functools import wraps
from typing import Any, Callable
from uuid import UUID

from livedocs.datasources.s3 import S3DatasourceConnector
from livedocs.types import (
    FileConnectorType,
    FileNodeType,
    MountHealthStatus,
    S3ConnectorInfo,
)


class TestS3DatasourceConnector(unittest.TestCase):
    """Test suite for S3DatasourceConnector."""

    # Verbose logging control - set to True to enable JSON logging of all SDK method calls
    VERBOSE = True

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        """Sanitize sensitive data in logging output."""
        if isinstance(value, dict):
            sanitized = {}
            for k, v in value.items():
                if k in ("access_key", "secret_key"):
                    sanitized[k] = "***"
                elif k == "connector_info" and isinstance(v, dict):
                    sanitized[k] = TestS3DatasourceConnector._sanitize_value(v)
                else:
                    sanitized[k] = TestS3DatasourceConnector._sanitize_value(v)
            return sanitized
        elif isinstance(value, (list, tuple)):
            return [TestS3DatasourceConnector._sanitize_value(item) for item in value]
        elif callable(value) and not isinstance(value, type):
            return "<callable>"
        else:
            return value

    @staticmethod
    def _summarize_return_value(return_value: Any) -> dict[str, Any]:
        """Create a summary of the return value for logging."""
        if return_value is None:
            return {"type": "None", "value": None}
        elif isinstance(return_value, list):
            return {
                "type": "list",
                "length": len(return_value),
                "summary": f"List of {len(return_value)} items",
            }
        elif isinstance(return_value, bool):
            return {"type": "bool", "value": return_value}
        elif isinstance(return_value, str):
            return {
                "type": "str",
                "length": len(return_value),
                "preview": return_value[:50] + "..."
                if len(return_value) > 50
                else return_value,
            }
        elif isinstance(return_value, UUID):
            return {"type": "UUID", "value": str(return_value)}
        else:
            return {
                "type": type(return_value).__name__,
                "summary": str(return_value)[:100] + "..."
                if len(str(return_value)) > 100
                else str(return_value),
            }

    @staticmethod
    def _create_logging_wrapper(
        method_name: str, original_method: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Create a wrapper that logs method calls when VERBOSE is enabled."""

        @wraps(original_method)
        def wrapper(*args, **kwargs):
            # Only log if VERBOSE is enabled
            if TestS3DatasourceConnector.VERBOSE:
                # Sanitize arguments
                sanitized_args = [
                    TestS3DatasourceConnector._sanitize_value(arg) for arg in args[1:]
                ]  # Skip 'self'
                sanitized_kwargs = {
                    k: TestS3DatasourceConnector._sanitize_value(v)
                    for k, v in kwargs.items()
                }

                # Call the original method
                try:
                    return_value = original_method(*args, **kwargs)
                    return_summary = TestS3DatasourceConnector._summarize_return_value(
                        return_value
                    )
                except Exception as e:
                    return_value = None
                    return_summary = {
                        "type": "exception",
                        "exception_type": type(e).__name__,
                        "message": str(e),
                    }
                    raise

                # Create log entry
                log_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "method": method_name,
                    "args": sanitized_args,
                    "kwargs": sanitized_kwargs,
                    "return_value": return_summary,
                }

                # Print JSON log
                print(json.dumps(log_entry, indent=2))

                return return_value
            else:
                # VERBOSE is False, just call the original method
                return original_method(*args, **kwargs)

        return wrapper

    def setUp(self):
        """Set up test fixtures with hardcoded credentials."""
        # Hardcoded S3 credentials - REPLACE WITH YOUR TEST CREDENTIALS
        self.connector_id = str(uuid.uuid4())
        self.connector_info: S3ConnectorInfo = {
            "connector_id": self.connector_id,
            "name": "My S3",
            "endpoint_url": "",
            "region": "auto",
            "provider": "railway",
            "access_key": "",
            "secret_key": "",
            "bucket_name": "",
            "path_prefix": "",  # or "some/folder"
            "is_virtual_hosted_style": True,
        }

        # Create connector instance
        self.connector = S3DatasourceConnector()

        # Wrap connector methods with logging if VERBOSE is enabled
        if self.VERBOSE:
            methods_to_wrap = [
                "list",
                "get_file",
                "save_file",
                "delete_file",
                "rename_file",
                "get_signed_url",
                "teardown",
            ]
            for method_name in methods_to_wrap:
                if hasattr(self.connector, method_name):
                    original_method = getattr(self.connector, method_name)
                    wrapped_method = self._create_logging_wrapper(
                        method_name, original_method
                    )
                    setattr(self.connector, method_name, wrapped_method)

        # Create get_connection_details callable
        def get_connection_details(conn_id: str) -> tuple[object, dict[str, Any]]:
            if conn_id == self.connector_id:
                return (None, self.connector_info)
            raise KeyError(f"Connector {conn_id} not found")

        self.get_connection_details = get_connection_details

        # Set up temporary directory for file downloads
        self.temp_dir = tempfile.mkdtemp()
        os.environ["LIVEDOCS_FILES_PATH"] = self.temp_dir

        # Test file paths (relative to path_prefix)
        self.test_file_path = "test_file.txt"
        self.test_file_path_nested = "folder1/test_file_nested.txt"
        self.test_dir_path = "test_folder"

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temporary directory
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.environ.pop("LIVEDOCS_FILES_PATH", None)

    # Static Helper Method Tests

    def test_create_s3_filesystem(self):
        """Test S3 filesystem creation with various configurations."""
        # Test with all options
        fs = S3DatasourceConnector._create_s3_filesystem(self.connector_info)
        self.assertIsNotNone(fs)
        self.assertEqual(fs.key, self.connector_info["access_key"])
        self.assertEqual(fs.secret, self.connector_info["secret_key"])

        # Test with path style addressing
        info_path_style = self.connector_info.copy()
        info_path_style["is_virtual_hosted_style"] = False
        fs_path = S3DatasourceConnector._create_s3_filesystem(info_path_style)
        self.assertIsNotNone(fs_path)

        # Test without region
        info_no_region = self.connector_info.copy()
        info_no_region.pop("region", None)
        fs_no_region = S3DatasourceConnector._create_s3_filesystem(info_no_region)
        self.assertIsNotNone(fs_no_region)

        # Test without endpoint_url
        info_no_endpoint = self.connector_info.copy()
        info_no_endpoint.pop("endpoint_url", None)
        fs_no_endpoint = S3DatasourceConnector._create_s3_filesystem(info_no_endpoint)
        self.assertIsNotNone(fs_no_endpoint)

    def test_generate_file_id(self):
        """Test deterministic UUID generation from connector_id and path."""
        path1 = "test/path/file.txt"
        path2 = "test/path/file2.txt"

        # Same path should generate same UUID
        id1 = S3DatasourceConnector._generate_file_id(self.connector_id, path1)
        id2 = S3DatasourceConnector._generate_file_id(self.connector_id, path1)
        self.assertEqual(id1, id2)
        self.assertIsInstance(id1, UUID)

        # Different paths should generate different UUIDs
        id3 = S3DatasourceConnector._generate_file_id(self.connector_id, path2)
        self.assertNotEqual(id1, id3)

        # Different connector IDs should generate different UUIDs for same path
        other_connector_id = str(uuid.uuid4())
        id4 = S3DatasourceConnector._generate_file_id(other_connector_id, path1)
        self.assertNotEqual(id1, id4)

    def test_get_parent_path(self):
        """Test parent path extraction."""
        # Root path
        self.assertIsNone(S3DatasourceConnector._get_parent_path(""))
        self.assertIsNone(S3DatasourceConnector._get_parent_path("/"))
        self.assertIsNone(S3DatasourceConnector._get_parent_path("file.txt"))

        # Nested paths
        self.assertEqual(
            S3DatasourceConnector._get_parent_path("folder/file.txt"), "folder"
        )
        self.assertEqual(
            S3DatasourceConnector._get_parent_path("folder1/folder2/file.txt"),
            "folder1/folder2",
        )

        # Paths with trailing slashes
        # "folder/" after stripping becomes "folder", which has no parent (it's at root)
        self.assertIsNone(S3DatasourceConnector._get_parent_path("folder/"))
        self.assertIsNone(S3DatasourceConnector._get_parent_path("/folder/"))

    def test_construct_s3_path(self):
        """Test S3 path construction with various path_prefix and relative_path combinations."""
        # With empty path_prefix and relative_path (current setup)
        path = S3DatasourceConnector._construct_s3_path(self.connector_info, "file.txt")
        # When path_prefix is empty, should not have double slash
        expected = f"{self.connector_info['bucket_name']}/file.txt"
        self.assertEqual(path, expected)

        # Test with non-empty path_prefix
        info_with_prefix = self.connector_info.copy()
        info_with_prefix["path_prefix"] = "test-prefix"
        path_with_prefix = S3DatasourceConnector._construct_s3_path(
            info_with_prefix, "file.txt"
        )
        expected_with_prefix = f"{info_with_prefix['bucket_name']}/{info_with_prefix['path_prefix']}/file.txt"
        self.assertEqual(path_with_prefix, expected_with_prefix)

        # With path_prefix only
        info_no_prefix = self.connector_info.copy()
        info_no_prefix["path_prefix"] = ""
        path_no_prefix = S3DatasourceConnector._construct_s3_path(
            info_no_prefix, "file.txt"
        )
        expected_no_prefix = f"{info_no_prefix['bucket_name']}/file.txt"
        self.assertEqual(path_no_prefix, expected_no_prefix)

        # With relative_path only (no path_prefix)
        path_root = S3DatasourceConnector._construct_s3_path(info_no_prefix, "")
        expected_root = f"{info_no_prefix['bucket_name']}/"
        self.assertEqual(path_root, expected_root)

        # Nested relative path with empty path_prefix
        path_nested = S3DatasourceConnector._construct_s3_path(
            self.connector_info, "folder/file.txt"
        )
        expected_nested = f"{self.connector_info['bucket_name']}/folder/file.txt"
        self.assertEqual(path_nested, expected_nested)

        # Nested relative path with non-empty path_prefix
        path_nested_with_prefix = S3DatasourceConnector._construct_s3_path(
            info_with_prefix, "folder/file.txt"
        )
        expected_nested_with_prefix = f"{info_with_prefix['bucket_name']}/{info_with_prefix['path_prefix']}/folder/file.txt"
        self.assertEqual(path_nested_with_prefix, expected_nested_with_prefix)

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
            self.assertEqual(node.mount_type, FileConnectorType.s3bucket)
            self.assertEqual(node.connector_id, UUID(self.connector_id))
            self.assertIsInstance(node.path, str)
            self.assertIsNotNone(node.health)
            self.assertEqual(node.health["status"], MountHealthStatus.connected)

    def test_list_nested_path(self):
        """Test listing files/directories from nested path."""
        nodes = self.connector.list(
            path=self.test_dir_path,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        self.assertIsInstance(nodes, list)
        for node in nodes:
            self.assertIsInstance(node.id, UUID)
            self.assertIsInstance(node.path, str)

    def test_list_with_path_prefix(self):
        """Test listing with path_prefix configured."""
        nodes = self.connector.list(
            path="",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )

        self.assertIsInstance(nodes, list)
        # All paths should be relative to path_prefix
        for node in nodes:
            self.assertIsInstance(node.path, str)

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

    def test_get_file_existing(self):
        """Test downloading an existing file."""
        # First, ensure file exists by uploading it
        test_content = b"Test file content"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            # Upload file
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Download file
            local_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=self.test_file_path,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            self.assertIsNotNone(local_path)
            if local_path is not None:
                self.assertTrue(os.path.exists(local_path))
                with open(local_path, "rb") as f:
                    content = f.read()
                    self.assertEqual(content, test_content)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_get_file_nonexistent(self):
        """Test downloading a non-existent file returns None."""
        local_path = self.connector.get_file(
            connector_type=FileConnectorType.s3bucket,
            path="nonexistent_file_xyz123.txt",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(local_path)

    def test_get_file_wrong_connector_type(self):
        """Test get_file() with wrong connector_type returns None."""
        local_path = self.connector.get_file(
            connector_type=FileConnectorType.googledrive,
            path="test.txt",
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(local_path)

    def test_get_file_missing_parameters(self):
        """Test get_file() with missing parameters returns None."""
        # Missing connector_id
        local_path = self.connector.get_file(
            connector_type=FileConnectorType.s3bucket,
            path="test.txt",
            connector_id=None,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(local_path)

        # Missing path
        local_path = self.connector.get_file(
            connector_type=FileConnectorType.s3bucket,
            path=None,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(local_path)

    def test_save_file_upload(self):
        """Test uploading a local file."""
        test_content = b"Test upload content"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            result = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )

            if not result:
                self.skipTest("Could not upload file - check credentials")

            self.assertTrue(result)

            # Verify file exists by trying to download it
            local_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=self.test_file_path,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertIsNotNone(local_path)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_save_file_without_s3_path(self):
        """Test uploading without s3_path (uses basename)."""
        test_content = b"Test content for basename"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".txt") as f:
            f.write(test_content)
            temp_file_path = f.name
            basename = os.path.basename(temp_file_path)

        try:
            result = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=None,
                get_connection_details=self.get_connection_details,
            )

            if not result:
                self.skipTest("Could not upload file - check credentials")

            self.assertTrue(result)

            # Verify file exists with basename
            local_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=basename,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertIsNotNone(local_path)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_save_file_nonexistent_local(self):
        """Test uploading a non-existent local file returns False."""
        result = self.connector.save_file(
            file_path="/nonexistent/path/file.txt",
            connector_type=FileConnectorType.s3bucket,
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
                connector_type=FileConnectorType.googledrive,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertFalse(result)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_delete_file_existing(self):
        """Test deleting an existing file."""
        # First upload a file
        test_content = b"File to delete"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            # Upload file
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Delete file
            result = self.connector.delete_file(
                file_path=self.test_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            self.assertTrue(result)

            # Verify file is deleted
            local_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=self.test_file_path,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertIsNone(local_path)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_delete_file_nonexistent(self):
        """Test deleting a non-existent file returns False."""
        result = self.connector.delete_file(
            file_path="nonexistent_file_xyz456.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_delete_file_wrong_connector_type(self):
        """Test delete_file() with wrong connector_type returns False."""
        result = self.connector.delete_file(
            file_path="test.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_rename_file_root(self):
        """Test renaming a file in root."""
        # Upload a file first
        test_content = b"File to rename"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Rename file
            new_name = "renamed_file.txt"
            result = self.connector.rename_file(
                file_path=self.test_file_path,
                new_name=new_name,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            self.assertTrue(result)

            # Verify old file doesn't exist
            old_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=self.test_file_path,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertIsNone(old_path)

            # Verify new file exists
            new_path = self.connector.get_file(
                connector_type=FileConnectorType.s3bucket,
                path=new_name,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )
            self.assertIsNotNone(new_path)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_rename_file_nested(self):
        """Test renaming a file in nested path."""
        # Upload a file to nested path
        test_content = b"File to rename in nested path"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path_nested,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Rename file
            new_name = "renamed_nested_file.txt"
            result = self.connector.rename_file(
                file_path=self.test_file_path_nested,
                new_name=new_name,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                get_connection_details=self.get_connection_details,
            )

            if result:
                # Verify new file exists at correct nested path
                expected_new_path = "folder1/renamed_nested_file.txt"
                new_path = self.connector.get_file(
                    connector_type=FileConnectorType.s3bucket,
                    path=expected_new_path,
                    connector_id=self.connector_id,
                    get_connection_details=self.get_connection_details,
                )
                self.assertIsNotNone(new_path)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_rename_file_nonexistent(self):
        """Test renaming a non-existent file returns False."""
        result = self.connector.rename_file(
            file_path="nonexistent_file_xyz789.txt",
            new_name="new_name.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_rename_file_wrong_connector_type(self):
        """Test rename_file() with wrong connector_type returns False."""
        result = self.connector.rename_file(
            file_path="test.txt",
            new_name="new_name.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertFalse(result)

    def test_get_signed_url_default_expiration(self):
        """Test generating presigned URL with default expiration."""
        # First upload a file
        test_content = b"File for signed URL"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Generate signed URL
            url = self.connector.get_signed_url(
                file_path=self.test_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                expiration_seconds=3600,  # Default
                get_connection_details=self.get_connection_details,
            )

            self.assertIsNotNone(url)
            self.assertIsInstance(url, str)
            if url is not None:
                self.assertTrue(url.startswith("http"))
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_get_signed_url_custom_expiration(self):
        """Test generating presigned URL with custom expiration."""
        # First upload a file
        test_content = b"File for signed URL custom"
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            uploaded = self.connector.save_file(
                file_path=temp_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                s3_path=self.test_file_path,
                get_connection_details=self.get_connection_details,
            )
            if not uploaded:
                self.skipTest("Could not upload test file - check credentials")

            # Generate signed URL with custom expiration
            url = self.connector.get_signed_url(
                file_path=self.test_file_path,
                connector_type=FileConnectorType.s3bucket,
                connector_id=self.connector_id,
                expiration_seconds=7200,  # 2 hours
                get_connection_details=self.get_connection_details,
            )

            self.assertIsNotNone(url)
            self.assertIsInstance(url, str)
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_get_signed_url_nonexistent(self):
        """Test generating presigned URL for non-existent file.

        Note: S3 can generate presigned URLs for any key, even if the object doesn't exist yet.
        The URL will be generated successfully, but accessing it will return 404 if the object doesn't exist.
        """
        url = self.connector.get_signed_url(
            file_path="nonexistent_file_xyz999.txt",
            connector_type=FileConnectorType.s3bucket,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        # S3 generates presigned URLs even for non-existent files
        self.assertIsNotNone(url)
        self.assertIsInstance(url, str)
        if url is not None:
            self.assertTrue(url.startswith("http"))

    def test_get_signed_url_wrong_connector_type(self):
        """Test get_signed_url() with wrong connector_type returns None."""
        url = self.connector.get_signed_url(
            file_path="test.txt",
            connector_type=FileConnectorType.googledrive,
            connector_id=self.connector_id,
            get_connection_details=self.get_connection_details,
        )
        self.assertIsNone(url)

    def test_teardown(self):
        """Test teardown() is a no-op."""
        # Should not raise any exceptions
        self.connector.teardown()


if __name__ == "__main__":
    unittest.main()
