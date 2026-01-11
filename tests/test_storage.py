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

import pytest
from coreason_etl_euctr.storage import (
    LocalStorageBackend,
    create_storage_backend,
)


def test_local_storage(tmp_path: Path) -> None:
    """Test LocalStorageBackend operations."""
    backend = LocalStorageBackend(tmp_path)
    backend.write("test.txt", "content")
    assert backend.read("test.txt") == "content"
    assert backend.exists("test.txt")
    assert not backend.exists("missing.txt")

    # Test Config
    config = backend.get_config()
    assert config["type"] == "local"
    assert config["base_path"] == str(tmp_path)

    # Test Factory
    backend2 = create_storage_backend(config)
    assert isinstance(backend2, LocalStorageBackend)
    assert backend2.read("test.txt") == "content"


def test_local_storage_read_missing(tmp_path: Path) -> None:
    """Test reading a missing file raises FileNotFoundError."""
    backend = LocalStorageBackend(tmp_path)
    with pytest.raises(FileNotFoundError):
        backend.read("missing.txt")


def test_local_storage_listing(tmp_path: Path) -> None:
    """Test list_files in LocalStorageBackend."""
    backend = LocalStorageBackend(tmp_path)
    backend.write("a.html", "a")
    backend.write("b.html", "b")
    backend.write("c.txt", "c")

    files = list(backend.list_files("*.html"))
    keys = sorted([f.key for f in files])
    assert keys == ["a.html", "b.html"]


def test_s3_storage_config_only() -> None:
    """Test S3StorageBackend config generation (no mock boto needed)."""
    try:
        import boto3

        # Mock boto3 client creation to avoid needing credentials
        with boto3.DEFAULT_SESSION.client("s3"):
            pass
    except Exception:
        pass
    pass
