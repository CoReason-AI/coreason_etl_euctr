# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from coreason_etl_euctr.storage import LocalStorageBackend, S3StorageBackend


def test_local_storage_backend(tmp_path: Path) -> None:
    """Test LocalStorageBackend CRUD operations."""
    backend = LocalStorageBackend(tmp_path)

    key = "test.txt"
    content = "Hello World"

    # Write
    backend.write(key, content)
    assert (tmp_path / key).exists()

    # Exists
    assert backend.exists(key)
    assert not backend.exists("missing.txt")

    # Read
    assert backend.read(key) == content

    # Read missing
    with pytest.raises(FileNotFoundError):
        backend.read("missing.txt")


def test_local_storage_list_files(tmp_path: Path) -> None:
    """Test LocalStorageBackend.list_files."""
    backend = LocalStorageBackend(tmp_path)

    # Create files
    files = ["a.html", "b.html", "c.txt"]
    for f in files:
        p = tmp_path / f
        p.write_text("content", encoding="utf-8")
        os.utime(p, (1000, 2000))

    # Test listing *.html
    results = list(backend.list_files("*.html"))
    assert len(results) == 2
    keys = sorted([r.key for r in results])
    assert keys == ["a.html", "b.html"]

    # Check mtime
    for r in results:
        assert r.mtime == 2000.0


def test_s3_storage_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test S3StorageBackend operations using mocks."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client

    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    bucket = "test-bucket"
    backend = S3StorageBackend(bucket_name=bucket)

    key = "folder/test.txt"
    content = "Hello S3"

    # Write
    backend.write(key, content)
    mock_client.put_object.assert_called_once_with(
        Bucket=bucket, Key=key, Body=content.encode("utf-8"), ContentType="text/html"
    )

    # Read Success
    mock_response = {"Body": MagicMock(read=lambda: content.encode("utf-8"))}
    mock_client.get_object.return_value = mock_response
    assert backend.read(key) == content
    mock_client.get_object.assert_called_with(Bucket=bucket, Key=key)

    # Read Missing (ClientError NoSuchKey)
    error_response = {"Error": {"Code": "NoSuchKey"}}
    mock_client.get_object.side_effect = ClientError(error_response, "GetObject")  # type: ignore[arg-type]
    with pytest.raises(FileNotFoundError):
        backend.read("missing.txt")

    # Read Error (Other)
    error_response_500 = {"Error": {"Code": "500"}}
    mock_client.get_object.side_effect = ClientError(error_response_500, "GetObject")  # type: ignore[arg-type]
    with pytest.raises(ClientError):
        backend.read("error.txt")

    # Exists Success
    mock_client.head_object.side_effect = None  # Reset side effect
    mock_client.head_object.return_value = {}
    assert backend.exists(key)

    # Exists Missing (ClientError 404)
    error_response_404 = {"Error": {"Code": "404"}}
    mock_client.head_object.side_effect = ClientError(error_response_404, "HeadObject")  # type: ignore[arg-type]
    assert not backend.exists("missing.txt")

    # Exists Error (Other - returns False currently)
    error_response_403 = {"Error": {"Code": "403"}}
    mock_client.head_object.side_effect = ClientError(error_response_403, "HeadObject")  # type: ignore[arg-type]
    assert not backend.exists("forbidden.txt")


def test_s3_storage_backend_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test S3StorageBackend with prefix."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend(bucket_name="bucket", prefix="data/bronze")
    backend.write("file.html", "content")

    mock_client.put_object.assert_called_once()
    call_args = mock_client.put_object.call_args[1]
    assert call_args["Key"] == "data/bronze/file.html"


def test_s3_storage_list_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test S3StorageBackend.list_files."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend(bucket_name="bucket", prefix="data")

    # Mock Paginator
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator

    # Mock Pages
    # Use timezone-aware timestamps well past 1970 to support Windows
    ts1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    ts3 = datetime(2025, 1, 3, 12, 0, 0, tzinfo=timezone.utc)
    ts_folder = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    page1 = {
        "Contents": [
            {"Key": "data/a.html", "LastModified": ts1},
            {"Key": "data/b.html", "LastModified": ts2},
            {"Key": "data/c.txt", "LastModified": ts3},
            {"Key": "data/", "LastModified": ts_folder},  # Folder placeholder
        ]
    }
    mock_paginator.paginate.return_value = [page1]

    results = list(backend.list_files("*.html"))

    # Assert c.txt (from page1) was filtered out
    for r in results:
        assert r.key != "c.txt" and r.key != "data/c.txt"

    # Expect 2 files
    assert len(results) == 2

    # Sort by key
    results.sort(key=lambda x: x.key)

    assert results[0].key == "a.html"
    assert results[0].mtime == ts1.timestamp()

    assert results[1].key == "b.html"
    assert results[1].mtime == ts2.timestamp()

    # Verify pagination call args
    mock_paginator.paginate.assert_called_with(Bucket="bucket", Prefix="data/")


def test_s3_storage_list_files_empty_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test S3StorageBackend.list_files with empty prefix to hit the else block.
    """
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend("bucket", prefix="")

    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator

    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    page = {"Contents": [{"Key": "root_file.html", "LastModified": ts}]}
    mock_paginator.paginate.return_value = [page]

    results = list(backend.list_files("*.html"))
    assert len(results) == 1
    assert results[0].key == "root_file.html"
