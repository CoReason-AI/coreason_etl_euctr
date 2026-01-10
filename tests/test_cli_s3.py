# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_euctr.main import main
from coreason_etl_euctr.storage import LocalStorageBackend, S3StorageBackend


@pytest.fixture  # type: ignore[misc]
def mock_run_bronze() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_euctr.main.run_bronze") as mock:
        yield mock


def test_cli_crawl_defaults_to_local(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that without S3 args, LocalStorageBackend is used."""
    # Ensure no env vars interfere
    monkeypatch.delenv("EUCTR_S3_BUCKET", raising=False)

    with patch("sys.argv", ["euctr-etl", "crawl", "--output-dir", "data/test"]):
        main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs

    # Verify storage_backend is LocalStorageBackend (or None, triggering default in run_bronze)
    if "storage_backend" in kwargs and kwargs["storage_backend"]:
        assert isinstance(kwargs["storage_backend"], LocalStorageBackend)
    else:
        # None defaults to Local in run_bronze
        pass


def test_cli_crawl_s3_args(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that --s3-bucket triggers S3StorageBackend."""
    monkeypatch.delenv("EUCTR_S3_BUCKET", raising=False)

    with patch("coreason_etl_euctr.storage.boto3"):  # Mock boto3 to avoid connection errors
        with patch("sys.argv", ["euctr-etl", "crawl", "--s3-bucket", "my-bucket", "--s3-prefix", "raw"]):
            main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs

    assert "storage_backend" in kwargs
    backend = kwargs["storage_backend"]
    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket_name == "my-bucket"
    assert backend.prefix == "raw"


def test_cli_crawl_s3_env_vars(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that EUCTR_S3_BUCKET triggers S3StorageBackend."""
    monkeypatch.setenv("EUCTR_S3_BUCKET", "env-bucket")
    monkeypatch.setenv("EUCTR_S3_REGION", "us-east-1")

    with patch("coreason_etl_euctr.storage.boto3"):
        with patch("sys.argv", ["euctr-etl", "crawl"]):
            main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs

    assert "storage_backend" in kwargs
    backend = kwargs["storage_backend"]
    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket_name == "env-bucket"


def test_cli_crawl_s3_cli_overrides_env(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that CLI args override Environment Variables."""
    monkeypatch.setenv("EUCTR_S3_BUCKET", "env-bucket")

    with patch("coreason_etl_euctr.storage.boto3"):
        with patch("sys.argv", ["euctr-etl", "crawl", "--s3-bucket", "cli-bucket"]):
            main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs

    backend = kwargs["storage_backend"]
    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket_name == "cli-bucket"


def test_cli_s3_mixed_config_env_and_cli(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test mixing Env (Bucket) and CLI (Prefix)."""
    monkeypatch.setenv("EUCTR_S3_BUCKET", "env-bucket")

    with patch("coreason_etl_euctr.storage.boto3"):
        # We only pass prefix via CLI, expect bucket to come from env
        with patch("sys.argv", ["euctr-etl", "crawl", "--s3-prefix", "cli-prefix"]):
            main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs

    backend = kwargs["storage_backend"]
    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket_name == "env-bucket"
    assert backend.prefix == "cli-prefix"


def test_cli_s3_region_only_ignored(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that providing only a region (without bucket) defaults to Local storage."""
    monkeypatch.delenv("EUCTR_S3_BUCKET", raising=False)
    monkeypatch.setenv("EUCTR_S3_REGION", "us-east-1")

    with patch("sys.argv", ["euctr-etl", "crawl"]):
        main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs
    # Should be None (default local)
    assert kwargs.get("storage_backend") is None


def test_cli_s3_empty_env_var(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that an empty EUCTR_S3_BUCKET environment variable is treated as unset."""
    monkeypatch.setenv("EUCTR_S3_BUCKET", "")

    with patch("sys.argv", ["euctr-etl", "crawl"]):
        main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs
    assert kwargs.get("storage_backend") is None


def test_cli_s3_complex_precedence(mock_run_bronze: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test complex precedence:
    - CLI Bucket overrides Env Bucket
    - Env Prefix is preserved (since CLI prefix not passed)
    - CLI Region overrides Env Region (not easily visible in backend object, but we assume S3 instantiated)
    """
    monkeypatch.setenv("EUCTR_S3_BUCKET", "env-bucket")
    monkeypatch.setenv("EUCTR_S3_PREFIX", "env-prefix")

    with patch("coreason_etl_euctr.storage.boto3"):
        with patch("sys.argv", ["euctr-etl", "crawl", "--s3-bucket", "cli-bucket"]):
            main()

    mock_run_bronze.assert_called_once()
    kwargs = mock_run_bronze.call_args.kwargs
    backend = kwargs["storage_backend"]

    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket_name == "cli-bucket"
    assert backend.prefix == "env-prefix"
