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

from coreason_etl_euctr.downloader import Downloader


def test_downloader_corrupt_meta_file(tmp_path: Path) -> None:
    """
    Test behavior when .meta file exists but contains invalid JSON/Format.
    The Downloader should treat it as 'no hash match' and overwrite/update it.
    """
    trial_id = "2023-CORRUPT"

    # Create corrupt meta
    meta_path = tmp_path / f"{trial_id}.meta"
    meta_path.write_text("THIS IS NOT KEY VALUE", encoding="utf-8")

    downloader = Downloader(output_dir=tmp_path)

    # Mock Response
    with patch("httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html>New Content</html>"
        mock_get.return_value = mock_resp

        with patch("time.sleep"):
            res = downloader.download_trial(trial_id)

    assert res is True
    # Verify .html written
    assert (tmp_path / f"{trial_id}.html").exists()
    # Verify .meta overwritten with valid content
    new_meta = meta_path.read_text(encoding="utf-8")
    assert "hash=" in new_meta
    assert "THIS IS NOT KEY VALUE" not in new_meta


def test_downloader_partial_content(tmp_path: Path) -> None:
    """
    Simulate a scenario where content is downloaded but looks suspicious (e.g. very short).
    Currently logic doesn't validate length, but this tests basic flow.
    """
    trial_id = "2023-SHORT"
    downloader = Downloader(output_dir=tmp_path)

    with patch("httpx.Client.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html></html>"  # Valid but empty-ish
        mock_get.return_value = mock_resp

        with patch("time.sleep"):
            # Current impl checks `if not response.text.strip():` -> log warning and continue.
            # So if text IS empty string, it returns False (fails) or tries next country?
            # It tries next country.
            # "<html></html>" is not empty string. It should save.
            res = downloader.download_trial(trial_id)

    assert res is True
    assert (tmp_path / f"{trial_id}.html").read_text(encoding="utf-8") == "<html></html>"
