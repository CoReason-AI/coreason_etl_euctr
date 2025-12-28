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

import pytest
import httpx
from coreason_etl_euctr.downloader import Downloader

@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)

def test_download_hashing_cdc_corrupted_meta(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """Test that corrupted metadata file is ignored."""
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "Content"
    mock_httpx_client.get.return_value = mock_resp

    # Create corrupted meta file
    meta = tmp_path / "123.meta"
    with open(meta, "wb") as f:
        f.write(b"\x80\x81\xFF") # invalid utf-8

    with patch("time.sleep"):
        downloader.download_trial("123")

    # Should proceed and overwrite
    assert meta.read_text(encoding="utf-8").startswith("source_country=")
