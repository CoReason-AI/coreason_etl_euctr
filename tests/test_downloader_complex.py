# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.downloader import Downloader


@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)


def test_download_updates_metadata_on_content_change(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """
    Test that if content changes on the server, the new content is saved
    and the hash in metadata is updated.
    """
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)
    trial_id = "2023-001"

    # 1. First Download (Version A)
    content_a = "<html>Version A</html>"
    hash_a = hashlib.sha256(content_a.encode("utf-8")).hexdigest()

    mock_resp_a = MagicMock()
    mock_resp_a.status_code = 200
    mock_resp_a.text = content_a
    mock_httpx_client.get.return_value = mock_resp_a

    with patch("time.sleep"):
        downloader.download_trial(trial_id)

    meta_path = tmp_path / f"{trial_id}.meta"
    assert f"hash={hash_a}" in meta_path.read_text(encoding="utf-8")

    # 2. Second Download (Version B - Changed)
    content_b = "<html>Version B</html>"
    hash_b = hashlib.sha256(content_b.encode("utf-8")).hexdigest()

    mock_resp_b = MagicMock()
    mock_resp_b.status_code = 200
    mock_resp_b.text = content_b
    mock_httpx_client.get.return_value = mock_resp_b

    with patch("time.sleep"):
        downloader.download_trial(trial_id)

    # Verify content updated
    html_path = tmp_path / f"{trial_id}.html"
    assert html_path.read_text(encoding="utf-8") == content_b

    # Verify hash updated
    assert f"hash={hash_b}" in meta_path.read_text(encoding="utf-8")


def test_download_recovers_from_missing_meta(tmp_path: Path, mock_httpx_client: MagicMock) -> None:
    """
    Test that if .html exists but .meta is missing, the downloader
    treats it as a fresh download and restores metadata.
    """
    downloader = Downloader(output_dir=tmp_path, client=mock_httpx_client)
    trial_id = "2023-002"
    content = "<html>Content</html>"

    # Create orphan HTML file
    (tmp_path / f"{trial_id}.html").write_text(content, encoding="utf-8")
    assert not (tmp_path / f"{trial_id}.meta").exists()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = content
    mock_httpx_client.get.return_value = mock_resp

    with patch("time.sleep"):
        downloader.download_trial(trial_id)

    # Verify meta created
    meta_path = tmp_path / f"{trial_id}.meta"
    assert meta_path.exists()
    assert "hash=" in meta_path.read_text(encoding="utf-8")


def test_save_to_disk_partial_failure(mock_httpx_client: MagicMock) -> None:
    """
    Test behavior when writing HTML succeeds but writing Meta fails.
    Current behavior: Raises IOError, HTML left (but meta missing).
    """
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False

    def side_effect_write(key: str, content: str) -> None:
        if key.endswith(".meta"):
            raise IOError("Disk Full on Meta")
        return None

    mock_storage.write.side_effect = side_effect_write

    downloader = Downloader(storage_backend=mock_storage, client=mock_httpx_client)
    trial_id = "2023-003"
    content = "<html>Content</html>"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = content
    mock_httpx_client.get.return_value = mock_resp

    with patch("time.sleep"):
        with pytest.raises(IOError, match="Disk Full on Meta"):
            downloader.download_trial(trial_id)

    # Verify write was called for HTML and Meta
    assert mock_storage.write.call_count == 2
    mock_storage.write.assert_any_call(f"{trial_id}.html", content)
