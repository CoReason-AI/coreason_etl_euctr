# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from coreason_etl_euctr.storage import LocalStorageBackend, S3StorageBackend


def test_local_storage_nested_directories(tmp_path: Path) -> None:
    """Test writing to deeply nested keys creates directories."""
    backend = LocalStorageBackend(tmp_path)
    key = "deep/nested/folder/structure/file.txt"
    content = "data"

    backend.write(key, content)

    target = tmp_path / "deep/nested/folder/structure/file.txt"
    assert target.exists()
    assert target.read_text(encoding="utf-8") == content
    assert backend.read(key) == content
    assert backend.exists(key)


def test_local_storage_special_characters(tmp_path: Path) -> None:
    """Test writing keys with special characters and Unicode."""
    backend = LocalStorageBackend(tmp_path)
    keys = [
        "file with spaces.txt",
        "unicode_★_file.txt",
        "bracket[test].txt",
    ]

    for key in keys:
        content = f"content for {key}"
        backend.write(key, content)
        assert (tmp_path / key).exists()
        assert backend.read(key) == content


def test_s3_storage_forbidden_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test handling of 403 Forbidden errors from S3."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend(bucket_name="bucket")

    # Mock 403 on Read
    error_403 = {"Error": {"Code": "403", "Message": "Forbidden"}}
    mock_client.get_object.side_effect = ClientError(error_403, "GetObject")

    with pytest.raises(ClientError):
        backend.read("secret.txt")

    # Mock 403 on Exists
    mock_client.head_object.side_effect = ClientError(error_403, "HeadObject")
    assert backend.exists("secret.txt") is False


def test_s3_storage_empty_and_large_content(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test writing empty string and large content to S3."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend(bucket_name="bucket")

    # Empty Content
    backend.write("empty.txt", "")
    mock_client.put_object.assert_called_with(Bucket="bucket", Key="empty.txt", Body=b"", ContentType="text/html")

    # Large Content
    large_content = "A" * 10_000
    backend.write("large.txt", large_content)
    mock_client.put_object.assert_called_with(
        Bucket="bucket", Key="large.txt", Body=large_content.encode("utf-8"), ContentType="text/html"
    )


def test_s3_storage_bad_encoding(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test S3 read failure when content is not valid UTF-8."""
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    monkeypatch.setattr("coreason_etl_euctr.storage.boto3", mock_boto3)

    backend = S3StorageBackend(bucket_name="bucket")

    # Return invalid bytes
    mock_response = {"Body": MagicMock(read=lambda: b"\x80\x81\xff")}
    mock_client.get_object.return_value = mock_response

    with pytest.raises(UnicodeDecodeError):
        backend.read("bad_encoding.txt")
