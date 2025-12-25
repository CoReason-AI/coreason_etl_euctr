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
    """Test coverage for case where label has no parent (unlikely but possible with fragments)."""
    # Create a disconnected string element just for logic reference in the test,
    # though we use mocks below.
    _ = NavigableString("EudraCT Number:")

    crawler = Crawler()
    # Mock soup.find_all to return a label with no parent
    with patch("coreason_etl_euctr.crawler.BeautifulSoup") as MockSoup:
        mock_soup_instance = MagicMock()
        MockSoup.return_value = mock_soup_instance

        mock_label = MagicMock()
        mock_label.parent = None  # Trigger the continue
        mock_soup_instance.find_all.return_value = [mock_label]

        ids = crawler.extract_ids("<html></html>")
        assert ids == []
