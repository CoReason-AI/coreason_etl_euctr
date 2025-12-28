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
from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.downloader import Downloader


@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)


def test_download_hashing_cdc_corrupted_meta(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that corrupted metadata file (invalid UTF-8) is ignored."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "Content"
    mock_httpx_client.get.return_value = mock_resp

    # Create corrupted meta file
    meta = tmp_path / "123.meta"
    with open(meta, "wb") as f:
        f.write(b"\x80\x81\xff")  # invalid utf-8

    with patch("time.sleep"):
        downloader.download_trial("123")

    # Should proceed and overwrite
    assert meta.read_text(encoding="utf-8").startswith("source_country=")


def test_download_hashing_cdc_malformed_meta_content(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that metadata with valid UTF-8 but invalid format is handled."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "New Content"
    mock_httpx_client.get.return_value = mock_resp

    # Create malformed meta (no key=value pairs)
    meta = tmp_path / "456.meta"
    meta.write_text("This is not a valid meta file format", encoding="utf-8")

    with patch("time.sleep"):
        downloader.download_trial("456")

    # Should overwrite
    content = meta.read_text(encoding="utf-8")
    assert "hash=" in content
    assert "source_country=" in content


def test_download_storage_permission_error(mock_httpx_client: MagicMock) -> None:
    """Test that PermissionError (subclass of OSError/Exception) is caught and raised."""
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.write.side_effect = PermissionError("Access Denied")

    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "Content"
    mock_httpx_client.get.return_value = mock_resp

    with patch("time.sleep"):
        with pytest.raises(PermissionError):
            downloader.download_trial("789")
