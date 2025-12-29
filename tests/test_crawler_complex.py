# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import Generator
from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.crawler import Crawler


@pytest.fixture  # type: ignore[misc]
def mock_client() -> Generator[MagicMock, None, None]:
    with patch("httpx.Client") as mock:
        yield mock.return_value


def test_harvest_transient_failure_recovery(mock_client: MagicMock) -> None:
    """
    Verify that harvest_ids (via fetch_search_page @retry) recovers from transient 500 errors.
    """
    # Setup: Page 1 fails twice with 500, then succeeds.
    error_resp = MagicMock(status_code=500)

    # We mock the client.get return value.
    # The first two return values should raise HTTPStatusError when raise_for_status() is called.
    mock_resp_fail = MagicMock()
    mock_resp_fail.status_code = 500
    mock_resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError("500", request=None, response=error_resp)

    mock_resp_ok = MagicMock()
    mock_resp_ok.status_code = 200
    mock_resp_ok.text = "<div><span>EudraCT Number:</span> 2023-001</div>"
    mock_resp_ok.raise_for_status.return_value = None

    mock_client.get.side_effect = [mock_resp_fail, mock_resp_fail, mock_resp_ok]

    crawler = Crawler(client=mock_client)

    # Patch sleep to speed up retry delays
    with patch("time.sleep"):
        # We assume tenacity's wait configuration allows at least 3 attempts.
        # Defaults are stop_after_attempt(3).
        results = list(crawler.harvest_ids(start_page=1, max_pages=1))

    assert results == ["2023-001"]
    assert mock_client.get.call_count == 3


def test_harvest_persistent_failure_skip(mock_client: MagicMock) -> None:
    """
    Verify that if a page fails consistently (exhausting retries),
    harvest_ids logs the error and continues to the next page.
    """
    crawler = Crawler(client=mock_client)

    # Mock fetch_search_page to simulate exhausted retries (raising exception)
    # We test the harvest loop logic here, assuming fetch_search_page's retry logic works as tested above.
    with patch.object(crawler, "fetch_search_page") as mock_fetch:
        mock_fetch.side_effect = [
            httpx.HTTPStatusError("500 Permanent", request=None, response=None),  # Page 1 fails
            "<div><span>EudraCT Number:</span> 2023-002</div>",  # Page 2 succeeds
        ]

        results = list(crawler.harvest_ids(start_page=1, max_pages=2))

    assert results == ["2023-002"]
    assert mock_fetch.call_count == 2


def test_harvest_mixed_results_sequence(mock_client: MagicMock) -> None:
    """
    Verify complex flow: Success -> Error (Skipped) -> Success -> Empty (Stop).
    """
    crawler = Crawler(client=mock_client)

    with patch.object(crawler, "fetch_search_page") as mock_fetch:
        mock_fetch.side_effect = [
            "<div><span>EudraCT Number:</span> ID-1</div>",  # Page 1: OK
            Exception("Simulated Error"),  # Page 2: Error
            "<div><span>EudraCT Number:</span> ID-3</div>",  # Page 3: OK
            "<html>No Results</html>",  # Page 4: Empty -> Stop
            "<div><span>EudraCT Number:</span> ID-5</div>",  # Page 5: Should not be reached
        ]

        results = list(crawler.harvest_ids(start_page=1, max_pages=10))

    assert results == ["ID-1", "ID-3"]
    # Should stop after calling page 4
    assert mock_fetch.call_count == 4


def test_extract_ids_malformed_html() -> None:
    """Verify robustness against malformed or messy HTML."""
    crawler = Crawler()

    # Case 1: Random garbage
    assert crawler.extract_ids("fdjsklafjdslkafjdsl") == []

    # Case 2: Unclosed tags
    html = "<div><span>EudraCT Number: 2023-001</span>"
    assert crawler.extract_ids(html) == ["2023-001"]

    # Case 3: Deeply nested
    html = "<div><div><p>EudraCT Number: 2023-003</p></div></div>"
    assert crawler.extract_ids(html) == ["2023-003"]


def test_extract_ids_whitespace_variations() -> None:
    """Verify handling of whitespace and non-breaking spaces in labels."""
    crawler = Crawler()

    # Case 1: Non-breaking space in label (e.g. "EudraCT\u00a0Number:")
    # This simulates a potential edge case where simple string matching might fail
    # unless we normalize.
    html_nbsp = "<div><span>EudraCT\u00a0Number:</span> 2023-NBSP</div>"
    # If the current implementation is brittle, this might fail or return empty.
    # We want to verify behavior. Ideally it SHOULD pass if we are robust.
    # Current code: `lambda text: "EudraCT Number:" in text`
    # "EudraCT Number:" has a normal space (0x20).
    # "EudraCT\u00a0Number:" has 0xA0.
    # They are NOT equal. Expectation: This likely FAILS with current code.
    # We assert strictly what we want. If it fails, we fix the code.
    ids = crawler.extract_ids(html_nbsp)

    # Let's assert it works (Fail-First TDD)
    assert ids == ["2023-NBSP"]


def test_extract_ids_multiple_per_page() -> None:
    """Verify extraction of multiple IDs on a single page."""
    html = """
    <table>
        <tr><td>EudraCT Number: 2023-001</td></tr>
        <tr><td>EudraCT Number: 2023-002</td></tr>
    </table>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert sorted(ids) == ["2023-001", "2023-002"]
