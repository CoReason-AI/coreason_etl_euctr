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
from coreason_etl_euctr.crawler import Crawler


@pytest.fixture  # type: ignore[misc]
def crawler(tmp_path: Path) -> Crawler:
    return Crawler(output_dir=str(tmp_path), sleep_seconds=0.0)


def test_search_ids_success(crawler: Crawler) -> None:
    html_content = """
    <html>
        <table>
            <tr>
                <td>
                    <span class="label">EudraCT Number:</span> 2021-123456-78<br/>
                    <span>Sponsor: Acme</span>
                </td>
            </tr>
            <tr>
                <td>
                    <div>EudraCT Number: 2022-987654-32</div>
                </td>
            </tr>
        </table>
    </html>
    """
    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, text=html_content)
        mock_get.return_value.raise_for_status = MagicMock()

        ids = list(crawler.search_ids(max_pages=1))

        assert len(ids) == 2
        assert "2021-123456-78" in ids
        assert "2022-987654-32" in ids
        mock_get.assert_called_once()


def test_search_ids_http_error(crawler: Crawler) -> None:
    with patch("httpx.Client.get") as mock_get:
        mock_get.side_effect = httpx.HTTPError("Network Error")

        ids = list(crawler.search_ids(max_pages=1))
        assert ids == []


def test_download_trial_priority_first_success(crawler: Crawler) -> None:
    """Test that it stops after the first successful download (3rd)."""
    with patch("httpx.Client.get") as mock_get:
        # First call succeeds
        mock_get.return_value = MagicMock(status_code=200, text="<html>Content</html>")

        filepath = crawler.download_trial("2021-000000-00")

        assert filepath is not None
        assert filepath.exists()
        assert filepath.read_text(encoding="utf-8") == "<html>Content</html>"

        # Should verify it called with '3rd'
        args, _ = mock_get.call_args
        assert "/trial/2021-000000-00/3rd" in args[0]


def test_download_trial_fallback_gb(crawler: Crawler) -> None:
    """Test fallback to GB if 3rd is 404."""
    with patch("httpx.Client.get") as mock_get:
        # Setup side effects for consecutive calls
        response_404 = MagicMock(status_code=404)
        response_200 = MagicMock(status_code=200, text="<html>GB Content</html>")

        mock_get.side_effect = [response_404, response_200]

        filepath = crawler.download_trial("2021-000000-00")

        assert filepath is not None
        assert filepath.read_text(encoding="utf-8") == "<html>GB Content</html>"

        assert mock_get.call_count == 2
        # Check URLs
        calls = mock_get.call_args_list
        assert "/3rd" in calls[0][0][0]
        assert "/GB" in calls[1][0][0]


def test_download_trial_all_fail(crawler: Crawler) -> None:
    """Test return None if all countries fail."""
    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=404)

        filepath = crawler.download_trial("2021-000000-00")

        assert filepath is None
        assert mock_get.call_count == 3  # 3rd, GB, DE


def test_download_trial_http_error(crawler: Crawler) -> None:
    """Test handling of HTTP error (e.g. timeout) during download loop."""
    with patch("httpx.Client.get") as mock_get:
        mock_get.side_effect = httpx.HTTPError("Boom")

        filepath = crawler.download_trial("2021-000000-00")

        assert filepath is None
        assert mock_get.call_count == 3


def test_download_trial_500_error(crawler: Crawler) -> None:
    """Test handling of 500 error which triggers raise_for_status."""
    with patch("httpx.Client.get") as mock_get:
        # Setup: first call returns 500 and raise_for_status raises HTTPError
        # subsequent calls return 404 to end loop gracefully
        response_500 = MagicMock(status_code=500)
        response_500.raise_for_status.side_effect = httpx.HTTPError("500 Error")

        response_404 = MagicMock(status_code=404)

        mock_get.side_effect = [response_500, response_404, response_404]

        filepath = crawler.download_trial("2021-000000-00")

        assert filepath is None
        assert mock_get.call_count == 3
