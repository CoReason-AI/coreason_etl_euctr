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


# Mock response helper
def mock_response(status_code: int, text: str = "") -> httpx.Response:
    return httpx.Response(status_code=status_code, text=text, request=httpx.Request("GET", "http://test"))


class TestResilienceComplex:
    """Complex resilience scenarios for Crawler and Downloader."""

    def test_crawler_retry_on_timeout(self) -> None:
        """Test that Crawler retries on ReadTimeout and ConnectTimeout."""
        mock_client = MagicMock(spec=httpx.Client)
        # Sequence: ReadTimeout -> ConnectTimeout -> Success
        mock_client.get.side_effect = [
            httpx.ReadTimeout("Read timed out"),
            httpx.ConnectTimeout("Connect timed out"),
            mock_response(200, "<html>Success</html>"),
        ]

        crawler = Crawler(client=mock_client)
        html = crawler.fetch_search_page(page_num=1)

        assert html == "<html>Success</html>"
        assert mock_client.get.call_count == 3

    def test_downloader_retry_mixed_sequence_success(self, tmp_path: Path) -> None:
        """
        Test Downloader retrying a mixed sequence of errors for a single country.
        Sequence: 502 (Bad Gateway) -> ReadTimeout -> NetworkError -> 200 (Success).
        Note: Tenacity stop_after_attempt is 3. So 1st (502), 2nd (Timeout), 3rd (NetworkError) -> FAIL?
        Wait, default implementation has stop_after_attempt(3).
        If attempt 1 fails, attempt 2 fails, attempt 3 fails -> raises.
        So to succeed, we need success on attempt 3.
        Sequence: 502 -> Timeout -> Success.
        """
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # 3rd Country: 502 -> Timeout -> Success
        mock_client.get.side_effect = [
            mock_response(502),
            httpx.ReadTimeout("Timeout"),
            mock_response(200, "Content"),
        ]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-MIXED-01")

        assert result is True
        assert mock_client.get.call_count == 3
        # Ensure file saved
        assert (tmp_path / "2020-MIXED-01.html").read_text() == "Content"

    def test_downloader_exhaustion_mixed_sequence(self, tmp_path: Path) -> None:
        """
        Test Downloader exhausting retries on mixed errors and falling back.
        Scenario:
        - 3rd: 500 -> Timeout -> 503 (Exhausted, raises)
        - GB: 200 (Success)
        """
        mock_client = MagicMock(spec=httpx.Client)
        storage = LocalStorageBackend(tmp_path)

        # Setup side effects
        # 3rd Country attempts (limit 3):
        # 1. 500 (Retry)
        # 2. Timeout (Retry)
        # 3. 503 (Raise) -> Downloader catches and moves to next country

        # GB Country attempts:
        # 4. 200 (Success)

        # Mock raise_for_status for 500/503 because real objects are needed if logic uses them?
        # Downloader logic calls raise_for_status().
        # But here we simulate client.get returning these or raising exceptions.
        # Downloader._fetch_with_retry: response = client.get() -> response.raise_for_status()

        # We need to ensure client.get returns the response object so raise_for_status is called on it.
        # Or raises exception directly if it's a network/timeout error.

        mock_client.get.side_effect = [
            mock_response(500),  # Attempt 1
            httpx.ReadTimeout("Timeout"),  # Attempt 2 (Exception raised by get)
            mock_response(503),  # Attempt 3
            mock_response(200, "GB Content"),  # GB Attempt 1
        ]

        downloader = Downloader(client=mock_client, storage_backend=storage)
        result = downloader.download_trial("2020-MIXED-02")

        assert result is True
        assert mock_client.get.call_count == 4
        # Verify saved content is from GB
        assert (tmp_path / "2020-MIXED-02.html").read_text() == "GB Content"
        assert "source_country=GB" in (tmp_path / "2020-MIXED-02.meta").read_text()

    def test_crawler_max_retries_exhausted_timeout(self) -> None:
        """Test Crawler failing after max retries with Timeouts."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.side_effect = httpx.ReadTimeout("Persistent Timeout")

        crawler = Crawler(client=mock_client)

        # Should raise the last exception (ReadTimeout)
        with pytest.raises(httpx.ReadTimeout):
            crawler.fetch_search_page(page_num=1)

        assert mock_client.get.call_count >= 3
