# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.crawler import Crawler


def test_harvest_ids_propagates_exception() -> None:
    """
    Verify that harvest_ids raises an exception if the underlying fetch fails,
    instead of swallowing it. This ensures that the outer loop (in main.py)
    stops and doesn't mark the page as done (preserving the resume capability).
    """
    mock_client = MagicMock(spec=httpx.Client)
    crawler = Crawler(client=mock_client)

    # Mock fetch_search_page to raise an error
    # Since fetch_search_page is a method on Crawler, and we want to test harvest_ids calling it.
    # We can mock the fetch_search_page method directly on the instance or patch it.
    # However, fetch_search_page has a @retry decorator, so mocking it on the instance is trickier
    # if we want to bypass the retry or test the retry.
    # But harvest_ids calls self.fetch_search_page.

    with patch.object(crawler, "fetch_search_page", side_effect=ValueError("Network Error")):
        gen = crawler.harvest_ids(start_page=1, max_pages=1)

        with pytest.raises(ValueError, match="Network Error"):
            next(gen)


def test_harvest_ids_yields_correctly() -> None:
    """Verify normal behavior."""
    crawler = Crawler()

    # Mock fetch to return valid HTML structured as expected by extract_ids
    # Case 1: Same element
    html = "<html><body><div><span>EudraCT Number: 2022-000123-45</span></div></body></html>"
    with patch.object(crawler, "fetch_search_page", return_value=html):
        gen = crawler.harvest_ids(start_page=10, max_pages=1)
        page_num, ids = next(gen)
        assert page_num == 10
        assert "2022-000123-45" in ids
