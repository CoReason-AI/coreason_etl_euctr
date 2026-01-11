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
from unittest.mock import MagicMock, patch
from typing import Generator

import pytest
from coreason_etl_euctr.storage import S3StorageBackend


@pytest.fixture  # type: ignore[misc]
def mock_boto_client() -> Generator[MagicMock, None, None]:
    with patch("boto3.client") as mock:
        yield mock.return_value


def test_s3_list_files_empty_prefix(mock_boto_client: MagicMock) -> None:
    """Test list_files with empty prefix to cover 'else' branch."""
    backend = S3StorageBackend(bucket_name="bucket", prefix="")

    paginator = MagicMock()
    mock_boto_client.get_paginator.return_value = paginator

    # Mock pages
    page1 = {
        "Contents": [
            {"Key": "a.html", "LastModified": datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)},
        ]
    }

    paginator.paginate.return_value = [page1]

    files = list(backend.list_files("*.html"))
    assert files[0].key == "a.html"


def test_s3_list_files_prefix_mismatch(mock_boto_client: MagicMock) -> None:
    """
    Test list_files where prefix is set but key doesn't start with it.
    (Simulating weird S3 behavior or misconfiguration coverage)
    """
    backend = S3StorageBackend(bucket_name="bucket", prefix="data")

    paginator = MagicMock()
    mock_boto_client.get_paginator.return_value = paginator

    # Mock pages returning a key that doesn't start with 'data'
    # Normally S3 filters, but we mock the response.
    page1 = {
        "Contents": [
            {"Key": "other.html", "LastModified": datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)},
        ]
    }

    paginator.paginate.return_value = [page1]

    files = list(backend.list_files("*.html"))
    # Should use full key
    assert files[0].key == "other.html"


def test_s3_init_missing_boto3() -> None:
    """Test ImportError if boto3 is not installed."""
    with patch("coreason_etl_euctr.storage.boto3", None):
        with pytest.raises(ImportError, match="boto3 is required"):
            S3StorageBackend(bucket_name="bucket")
