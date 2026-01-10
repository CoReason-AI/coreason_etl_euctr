# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock

import httpx
import pytest

from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.utils import is_retryable_error


# Mock response helper
def mock_response(status_code: int) -> httpx.Response:
    return httpx.Response(status_code=status_code, request=httpx.Request("GET", "http://test"))


class TestResilienceComplex:
    """
    Complex and edge-case tests for resilience logic.
    """

    def test_mixed_failure_sequence(self) -> None:
        """
        Test a chaotic sequence of different retryable errors:
        NetworkError -> 503 -> 429 -> Success.
        """
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.side_effect = [
            httpx.NetworkError("Connection reset"),
            mock_response(503),
            mock_response(429),
            mock_response(200),  # Success on 4th attempt
        ]

        # Crawler has stop_after_attempt(3) by default.
        # We need to ensure we can handle at least 3.
        # Wait, if we have 3 errors then success is 4th call.
        # If stop=3, it will fail after 3rd error.
        # Let's adjust the mock to succeed on 3rd attempt:
        # Error -> Error -> Success.

        mock_client.get.side_effect = [
            httpx.NetworkError("Connection reset"),
            mock_response(503),
            httpx.Response(200, text="Success", request=httpx.Request("GET", "/")),
        ]

        crawler = Crawler(client=mock_client)
        result = crawler.fetch_search_page(page_num=1)

        assert result == "Success"
        assert mock_client.get.call_count == 3

    def test_boundary_status_codes(self) -> None:
        """
        Test boundary values for status codes in is_retryable_error.
        """
        # 499 (Client Closed Request) -> Should NOT retry (it's < 500)
        assert (
            is_retryable_error(httpx.HTTPStatusError("499", request=MagicMock(), response=mock_response(499))) is False
        )

        # 500 -> Retry
        assert (
            is_retryable_error(httpx.HTTPStatusError("500", request=MagicMock(), response=mock_response(500))) is True
        )

        # 599 -> Retry
        assert (
            is_retryable_error(httpx.HTTPStatusError("599", request=MagicMock(), response=mock_response(599))) is True
        )

        # 600 -> Should NOT retry (strictly < 600 check usually, depending on impl)
        # utils.py: 500 <= code < 600. So 600 is False.
        assert (
            is_retryable_error(httpx.HTTPStatusError("600", request=MagicMock(), response=mock_response(600))) is False
        )

    def test_specific_network_exceptions(self) -> None:
        """
        Verify specific subclasses of NetworkError trigger retry.
        """
        # ConnectError
        assert is_retryable_error(httpx.ConnectError("Connection failed")) is True

        # ReadError
        assert is_retryable_error(httpx.ReadError("Read failed")) is True

        # WriteError
        assert is_retryable_error(httpx.WriteError("Write failed")) is True

        # PoolTimeout (subclass of TimeoutException)
        assert is_retryable_error(httpx.PoolTimeout("Pool full")) is True

    def test_unexpected_exceptions_fail_fast(self) -> None:
        """
        Ensure unexpected exceptions (like parsing errors or logic errors)
        do NOT trigger retry and bubble up immediately.
        """
        # ValueError
        assert is_retryable_error(ValueError("Parsing error")) is False

        # AttributeError
        assert is_retryable_error(AttributeError("Missing attr")) is False

    def test_crawler_max_retries_mixed_errors(self) -> None:
        """
        Verify that after N mixed errors, it finally raises the LAST exception.
        """
        mock_client = MagicMock(spec=httpx.Client)
        # 3 failures: Network -> 500 -> 429. All retryable.
        # Should stop after 3 attempts.
        mock_client.get.side_effect = [
            httpx.NetworkError("Net Fail"),
            mock_response(500),
            mock_response(429),
        ]

        crawler = Crawler(client=mock_client)

        # It should raise the *last* exception (HTTPStatusError 429)
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            crawler.fetch_search_page(page_num=1)

        assert excinfo.value.response.status_code == 429
        assert mock_client.get.call_count == 3
