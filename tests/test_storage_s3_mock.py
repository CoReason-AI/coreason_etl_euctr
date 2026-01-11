# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import datetime
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from coreason_etl_euctr.storage import S3StorageBackend, create_storage_backend


@pytest.fixture  # type: ignore[misc]
def mock_boto_client() -> Generator[MagicMock, None, None]:
    with patch("boto3.client") as mock:
        yield mock.return_value


def test_s3_init_and_config(mock_boto_client: MagicMock) -> None:
    """Test initialization and config serialization."""
    backend = S3StorageBackend(bucket_name="my-bucket", prefix="data", region_name="us-east-1")

    assert backend.bucket_name == "my-bucket"
    assert backend.prefix == "data"
    assert backend.region_name == "us-east-1"

    config = backend.get_config()
    assert config == {
        "type": "s3",
        "bucket_name": "my-bucket",
        "prefix": "data",
        "region_name": "us-east-1",
    }

    # Test recreation
    backend2 = create_storage_backend(config)
    assert isinstance(backend2, S3StorageBackend)
    assert backend2.bucket_name == "my-bucket"


def test_s3_write(mock_boto_client: MagicMock) -> None:
    """Test write operation."""
    backend = S3StorageBackend(bucket_name="bucket")
    backend.write("file.txt", "content")

    mock_boto_client.put_object.assert_called_once_with(
        Bucket="bucket", Key="file.txt", Body=b"content", ContentType="text/html"
    )


def test_s3_write_with_prefix(mock_boto_client: MagicMock) -> None:
    """Test write with prefix."""
    backend = S3StorageBackend(bucket_name="bucket", prefix="sub/folder")
    backend.write("file.txt", "content")

    mock_boto_client.put_object.assert_called_once_with(
        Bucket="bucket", Key="sub/folder/file.txt", Body=b"content", ContentType="text/html"
    )


def test_s3_read_success(mock_boto_client: MagicMock) -> None:
    """Test successful read."""
    backend = S3StorageBackend(bucket_name="bucket")

    # Mock response body stream
    mock_body = MagicMock()
    mock_body.read.return_value = b"content"
    mock_boto_client.get_object.return_value = {"Body": mock_body}

    content = backend.read("file.txt")
    assert content == "content"
    mock_boto_client.get_object.assert_called_once_with(Bucket="bucket", Key="file.txt")


def test_s3_read_not_found(mock_boto_client: MagicMock) -> None:
    """Test read raising FileNotFoundError on 404."""
    backend = S3StorageBackend(bucket_name="bucket")

    error_response = {"Error": {"Code": "NoSuchKey"}}
    mock_boto_client.get_object.side_effect = ClientError(error_response, "GetObject")

    with pytest.raises(FileNotFoundError):
        backend.read("missing.txt")


def test_s3_read_other_error(mock_boto_client: MagicMock) -> None:
    """Test read raising other ClientErrors."""
    backend = S3StorageBackend(bucket_name="bucket")

    error_response = {"Error": {"Code": "AccessDenied"}}
    mock_boto_client.get_object.side_effect = ClientError(error_response, "GetObject")

    with pytest.raises(ClientError):
        backend.read("file.txt")


def test_s3_exists_true(mock_boto_client: MagicMock) -> None:
    """Test exists returns True."""
    backend = S3StorageBackend(bucket_name="bucket")
    assert backend.exists("file.txt") is True
    mock_boto_client.head_object.assert_called_once_with(Bucket="bucket", Key="file.txt")


def test_s3_exists_false(mock_boto_client: MagicMock) -> None:
    """Test exists returns False on 404."""
    backend = S3StorageBackend(bucket_name="bucket")

    error_response = {"Error": {"Code": "404"}}
    mock_boto_client.head_object.side_effect = ClientError(error_response, "HeadObject")

    assert backend.exists("missing.txt") is False


def test_s3_list_files(mock_boto_client: MagicMock) -> None:
    """Test list_files pagination."""
    backend = S3StorageBackend(bucket_name="bucket", prefix="data")

    paginator = MagicMock()
    mock_boto_client.get_paginator.return_value = paginator

    # Mock pages
    page1 = {
        "Contents": [
            {"Key": "data/a.html", "LastModified": datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)},
            {"Key": "data/sub/b.html", "LastModified": datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)},
            {"Key": "data/ignore.txt", "LastModified": datetime.datetime(2023, 1, 3, tzinfo=datetime.timezone.utc)},
            {
                "Key": "data/",
                "LastModified": datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
            },  # Prefix itself
        ]
    }

    paginator.paginate.return_value = [page1]

    files = list(backend.list_files("*.html"))

    # Should find a.html and b.html (recursive list_objects_v2 behavior assumption in backend logic)
    # Backend code logic:
    # if prefix and key.startswith(prefix): relative_key = key[len(prefix):]

    # Expected relative keys:
    # data/a.html -> a.html
    # data/sub/b.html -> sub/b.html

    keys = sorted([f.key for f in files])
    assert keys == ["a.html", "sub/b.html"]
