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

        html = crawler.fetch_search_page(page_num=1)

        assert html == "<html>Success</html>"
        assert mock_client.get.call_count == 3

    def test_fetch_search_page_retry_failure(self) -> None:
        """Test that crawler gives up after max retries on 500."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = mock_response(500)

        crawler = Crawler(client=mock_client)

        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_search_page(page_num=1)

        assert mock_client.get.call_count >= 3

    def test_fetch_search_page_retry_429(self) -> None:
        """Test that crawler retries on 429."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.side_effect = [
            mock_response(429),
            mock_response(200, "<html>Success</html>"),
        ]

        crawler = Crawler(client=mock_client)

        html = crawler.fetch_search_page(page_num=1)

        assert html == "<html>Success</html>"
        assert mock_client.get.call_count == 2

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
        assert mock_client.get.call_count == 2

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

    def test_download_trial_retry_remote_protocol_error(self, tmp_path: Path) -> None:
        """Test retry on RemoteProtocolError."""
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # '3rd': RemoteProtocolError, 200
        mock_client.get.side_effect = [
            httpx.RemoteProtocolError("Bad Protocol"),
            mock_response(200, "content"),
        ]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-123456-78")

        assert result is True
        assert mock_client.get.call_count == 2


class TestIsRetryableError:
    """Explicit tests for the predicate."""

    def test_status_codes(self) -> None:
        # 5xx
        assert (
            is_retryable_error(httpx.HTTPStatusError("500", request=MagicMock(), response=mock_response(500))) is True
        )
        assert (
            is_retryable_error(httpx.HTTPStatusError("503", request=MagicMock(), response=mock_response(503))) is True
        )

        # 429
        assert (
            is_retryable_error(httpx.HTTPStatusError("429", request=MagicMock(), response=mock_response(429))) is True
        )

        # Non-retryable
        assert (
            is_retryable_error(httpx.HTTPStatusError("404", request=MagicMock(), response=mock_response(404))) is False
        )
        assert (
            is_retryable_error(httpx.HTTPStatusError("403", request=MagicMock(), response=mock_response(403))) is False
        )
        assert (
            is_retryable_error(httpx.HTTPStatusError("200", request=MagicMock(), response=mock_response(200))) is False
        )

    def test_exceptions(self) -> None:
        assert is_retryable_error(httpx.NetworkError("Net")) is True
        assert is_retryable_error(httpx.TimeoutException("Timeout")) is True
        assert is_retryable_error(httpx.RemoteProtocolError("Proto")) is True

        assert is_retryable_error(ValueError("Generic")) is False
        assert is_retryable_error(RuntimeError("Runtime")) is False
