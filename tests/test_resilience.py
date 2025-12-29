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

import httpx
import pytest
from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.downloader import Downloader
from coreason_etl_euctr.storage import LocalStorageBackend
from coreason_etl_euctr.utils import is_retryable_error


# Mock response helper
def mock_response(status_code: int, text: str = "") -> httpx.Response:
    return httpx.Response(status_code=status_code, text=text, request=httpx.Request("GET", "http://test"))


class TestCrawlerResilience:
    def test_fetch_search_page_retry_success(self) -> None:
        """Test that crawler retries on 500 and eventually succeeds."""
        mock_client = MagicMock(spec=httpx.Client)
        # Sequence: 500, 503, 200
        mock_client.get.side_effect = [
            mock_response(500),
            mock_response(503),
            mock_response(200, "<html>Success</html>"),
        ]

        crawler = Crawler(client=mock_client)
        # We need to patch the wait strategy to speed up tests or just rely on default if small?
        # Ideally we patch tenacity.wait.
        # But for unit test, if we implement it, we can assert call count.

        html = crawler.fetch_search_page(page_num=1)

        assert html == "<html>Success</html>"
        assert mock_client.get.call_count == 3

    def test_fetch_search_page_retry_failure(self) -> None:
        """Test that crawler gives up after max retries on 500."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = mock_response(500)

        crawler = Crawler(client=mock_client)

        # With reraise=True, it raises the underlying exception (HTTPStatusError)
        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_search_page(page_num=1)

        # Assuming stop_after_attempt(3)
        assert mock_client.get.call_count >= 3

    def test_fetch_search_page_no_retry_404(self) -> None:
        """Test that crawler fails fast on 404."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = mock_response(404)

        crawler = Crawler(client=mock_client)

        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_search_page(page_num=1)

        assert mock_client.get.call_count == 1


class TestDownloaderResilience:
    def test_download_trial_retry_success(self, tmp_path: Path) -> None:
        """Test that downloader retries on 500 for a country."""
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # For '3rd': 500, 200
        mock_client.get.side_effect = [mock_response(500), mock_response(200, "content")]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-123456-78")

        assert result is True
        # Should have called get twice for the same URL (3rd)
        assert mock_client.get.call_count == 2
        # Verify arguments
        calls = mock_client.get.call_args_list
        assert "3rd" in calls[0][0][0]
        assert "3rd" in calls[1][0][0]

    def test_download_trial_fail_fast_404(self, tmp_path: Path) -> None:
        """Test that downloader does not retry on 404, proceeds to next country."""
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # '3rd' -> 404 (No retry)
        # 'GB' -> 200
        mock_client.get.side_effect = [mock_response(404), mock_response(200, "content")]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-123456-78")

        assert result is True
        assert mock_client.get.call_count == 2

        calls = mock_client.get.call_args_list
        assert "3rd" in calls[0][0][0]
        assert "GB" in calls[1][0][0]

    def test_download_trial_retry_network_error(self, tmp_path: Path) -> None:
        """Test retry on NetworkError."""
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # '3rd': NetworkError, 200
        mock_client.get.side_effect = [httpx.NetworkError("Fail"), mock_response(200, "content")]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-123456-78")

        assert result is True
        assert mock_client.get.call_count == 2


def test_is_retryable_error_generic() -> None:
    """Test is_retryable_error with generic exceptions."""
    assert is_retryable_error(ValueError("Generic error")) is False
    assert is_retryable_error(RuntimeError("Runtime error")) is False
