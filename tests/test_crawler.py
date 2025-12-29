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
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from bs4 import NavigableString
from coreason_etl_euctr.crawler import Crawler


@pytest.fixture  # type: ignore[misc]
def mock_httpx_client() -> Generator[MagicMock, None, None]:
    with patch("httpx.Client") as mock:
        client_instance = MagicMock()
        mock.return_value = client_instance
        yield client_instance


def test_crawler_initialization(mock_httpx_client: MagicMock) -> None:
    """Test that Crawler initializes with a client."""
    crawler = Crawler()
    assert crawler.client is not None


def test_fetch_search_page_success(mock_httpx_client: MagicMock) -> None:
    """Test successful page fetch including sleep call."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>Success</html>"
    mock_httpx_client.get.return_value = mock_response

    with patch("time.sleep") as mock_sleep:
        crawler = Crawler(client=mock_httpx_client)
        html = crawler.fetch_search_page(page_num=1)

        mock_sleep.assert_called_once_with(1)

    assert html == "<html>Success</html>"
    mock_httpx_client.get.assert_called_once()


def test_fetch_search_page_with_dates(mock_httpx_client: MagicMock) -> None:
    """Test fetching page with date filters."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>Success</html>"
    mock_httpx_client.get.return_value = mock_response

    with patch("time.sleep"):
        crawler = Crawler(client=mock_httpx_client)
        crawler.fetch_search_page(page_num=1, date_from="2023-01-01", date_to="2023-12-31")

    # Verify query params
    mock_httpx_client.get.assert_called_once()
    args, kwargs = mock_httpx_client.get.call_args
    assert kwargs["params"]["dateFrom"] == "2023-01-01"
    assert kwargs["params"]["dateTo"] == "2023-12-31"


def test_fetch_search_page_failure(mock_httpx_client: MagicMock) -> None:
    """Test failure during page fetch."""
    mock_response = MagicMock()
    mock_response.status_code = 500

    def raise_error() -> None:
        raise httpx.HTTPStatusError("Server Error", request=MagicMock(), response=mock_response)

    mock_response.raise_for_status.side_effect = raise_error
    mock_httpx_client.get.return_value = mock_response

    crawler = Crawler(client=mock_httpx_client)

    with pytest.raises(httpx.HTTPStatusError):
        with patch("time.sleep"):  # Speed up test
            crawler.fetch_search_page(page_num=1)


def test_extract_ids_simple() -> None:
    """Test ID extraction from simple simulated HTML."""
    html = """
    <div>
        <div class="result">
            <span>EudraCT Number:</span> <span>2004-000015-26</span>
        </div>
        <div class="result">
            <span>EudraCT Number:</span> <span>2023-123456-78</span>
        </div>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == ["2004-000015-26", "2023-123456-78"]


def test_extract_ids_text_variant() -> None:
    """Test ID extraction where ID is in the same text block."""
    html = """
    <div>
        <div class="result">
            <span>EudraCT Number: 2004-000015-26</span>
        </div>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == ["2004-000015-26"]


def test_extract_ids_label_text_node_variant() -> None:
    """Test ID extraction where ID is a text node sibling (common legacy HTML)."""
    html = """
    <div>
        <b>EudraCT Number:</b> 2004-001234-56<br/>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == ["2004-001234-56"]


def test_extract_ids_empty() -> None:
    """Test extraction with no IDs."""
    html = "<html><body>No results here</body></html>"
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == []


def test_extract_ids_deduplication() -> None:
    """Test that duplicate IDs on the same page are removed."""
    html = """
    <div>
        <span>EudraCT Number:</span> <span>2004-000015-26</span>
        <span>EudraCT Number:</span> <span>2004-000015-26</span>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == ["2004-000015-26"]


def test_extract_ids_orphaned_label() -> None:
    """Test coverage for case where label has no parent."""
    _ = NavigableString("EudraCT Number:")

    crawler = Crawler()
    with patch("coreason_etl_euctr.crawler.BeautifulSoup") as MockSoup:
        mock_soup_instance = MagicMock()
        MockSoup.return_value = mock_soup_instance

        mock_label = MagicMock()
        mock_label.parent = None
        mock_soup_instance.find_all.return_value = [mock_label]

        ids = crawler.extract_ids("<html></html>")
        assert ids == []


def test_extract_ids_comment_ignored() -> None:
    """Test that HTML comments containing the label are ignored."""
    html = """
    <div>
        <!-- EudraCT Number: 9999-999999-99 -->
        <span>Real ID</span>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    # Should NOT find the ID in the comment
    assert ids == []


def test_extract_ids_unicode_handling() -> None:
    """Test handling of unicode spaces and characters."""
    # Uses non-breaking space \u00A0
    html = """
    <div>
        <span>EudraCT Number:</span>\u00a0\u00a0<span>2004-001234-56</span>
    </div>
    """
    crawler = Crawler()
    ids = crawler.extract_ids(html)
    assert ids == ["2004-001234-56"]


def test_harvest_ids_pagination(mock_httpx_client: MagicMock) -> None:
    """Test that harvest_ids iterates multiple pages and respects max_pages."""
    # Mock responses for 2 pages
    page1 = """<div><span>EudraCT Number:</span> <span>ID-1</span></div>"""
    page2 = """<div><span>EudraCT Number:</span> <span>ID-2</span></div>"""

    mock_httpx_client.get.side_effect = [
        MagicMock(status_code=200, text=page1),
        MagicMock(status_code=200, text=page2),
    ]

    crawler = Crawler(client=mock_httpx_client)

    with patch("time.sleep"):
        ids = list(crawler.harvest_ids(start_page=1, max_pages=2))

    assert ids == ["ID-1", "ID-2"]
    assert mock_httpx_client.get.call_count == 2
    # Verify page params
    call_args = mock_httpx_client.get.call_args_list
    assert call_args[0][1]["params"]["page"] == "1"
    assert call_args[1][1]["params"]["page"] == "2"


def test_harvest_ids_stops_on_empty_page(mock_httpx_client: MagicMock) -> None:
    """Test that harvest_ids stops if a page has no IDs."""
    page1 = """<div><span>EudraCT Number:</span> <span>ID-1</span></div>"""
    page2 = "<html><body>No results</body></html>"  # Empty of IDs

    mock_httpx_client.get.side_effect = [
        MagicMock(status_code=200, text=page1),
        MagicMock(status_code=200, text=page2),
    ]

    crawler = Crawler(client=mock_httpx_client)

    with patch("time.sleep"):
        ids = list(crawler.harvest_ids(start_page=1, max_pages=10))

    # Should only get ID-1, then stop at page 2
    assert ids == ["ID-1"]
    assert mock_httpx_client.get.call_count == 2


def test_harvest_ids_handles_exception(mock_httpx_client: MagicMock) -> None:
    """Test that harvest_ids continues or handles exception on a page."""
    page1 = """<div><span>EudraCT Number:</span> <span>ID-1</span></div>"""
    page3 = """<div><span>EudraCT Number:</span> <span>ID-3</span></div>"""

    # Mock: Page 1 success, Page 2 fails (exception), Page 3 success
    # Note: harvest_ids catches Exception and logs error, then continues.
    # However, fetch_search_page raises HTTPStatusError which is an Exception.

    resp1 = MagicMock(status_code=200, text=page1)
    resp3 = MagicMock(status_code=200, text=page3)

    # Page 2 failure
    error_resp = MagicMock(status_code=500)

    def raise_http_error(*args: object, **kwargs: object) -> None:
        raise httpx.HTTPStatusError("500 Error", request=MagicMock(), response=error_resp)

    # We need to mock the sequence of calls to client.get
    # Since fetch_search_page has @retry, it might try multiple times.
    # We'll mock fetch_search_page directly to avoid testing retry logic here (tested separately).

    crawler = Crawler(client=mock_httpx_client)

    with patch.object(crawler, "fetch_search_page") as mock_fetch:
        mock_fetch.side_effect = [
            page1,
            Exception("Simulated Fetch Error"),
            page3,
        ]

        ids = list(crawler.harvest_ids(start_page=1, max_pages=3))

    # Should get ID-1 and ID-3. Page 2 skipped.
    assert ids == ["ID-1", "ID-3"]
    assert mock_fetch.call_count == 3


def test_harvest_ids_passes_dates(mock_httpx_client: MagicMock) -> None:
    """Test that harvest_ids passes date filters correctly."""
    crawler = Crawler(client=mock_httpx_client)
    mock_httpx_client.get.return_value = MagicMock(status_code=200, text="<html></html>")

    with patch("time.sleep"):
        list(crawler.harvest_ids(start_page=1, max_pages=1, date_from="2023-01-01"))

    call_args = mock_httpx_client.get.call_args
    assert call_args[1]["params"]["dateFrom"] == "2023-01-01"
