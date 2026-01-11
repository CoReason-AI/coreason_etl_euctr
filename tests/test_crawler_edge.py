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

from coreason_etl_euctr.crawler import Crawler


def test_crawler_extract_ids_invalid_date() -> None:
    """Test date parsing failure fallback."""
    crawler = Crawler()

    # HTML with valid structure but invalid date string
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>2023-001</td></tr>
        <tr><td>Date record first entered:</td><td>Invalid Date String</td></tr>
    </table>
    """

    # Should extract ID but Date should be None (exception caught)
    ids = crawler.extract_ids(html)
    assert len(ids) == 1
    assert ids[0][0] == "2023-001"
    assert ids[0][1] is None


def test_crawler_extract_ids_label_split_logic() -> None:
    """Cover lines where ID extraction logic branches."""
    crawler = Crawler()

    # Case: ID in same node but nothing after split (shouldn't happen with valid split but coverage)
    # "EudraCT Number:" -> split -> [""] -> cleaned empty
    html = "<div>EudraCT Number:</div>"
    assert crawler.extract_ids(html) == []

    # Case: Sibling has text but after normalize it's empty?
    # <span>ID:</span> <span>   </span>
    html2 = "<div><span>EudraCT Number:</span> <span> </span></div>"
    assert crawler.extract_ids(html2) == []


def test_crawler_lambda_edge() -> None:
    """Test edge cases for lambda in find_all."""
    crawler = Crawler()
    # Empty comment might trigger lambda with empty string?
    html = "<div><!-- --></div>"
    crawler.extract_ids(html)

    # Empty text node
    html2 = "<div> </div>"
    crawler.extract_ids(html2)


def test_crawler_date_container_whitespace() -> None:
    """
    Test extraction of date where there is significant whitespace/siblings between label and value.
    Covers the 'while next_node ...' loop in _extract_date_from_container.
    """
    crawler = Crawler()
    html = """
    <table>
        <tr>
            <td>EudraCT Number:</td><td>2023-999</td>
        </tr>
        <tr>
            <td>Date record first entered:</td>
            <!-- Comment in between -->

            <td>2023-12-31</td>
        </tr>
    </table>
    """
    ids = crawler.extract_ids(html)
    assert len(ids) == 1
    assert ids[0][0] == "2023-999"
    assert str(ids[0][1]) == "2023-12-31"


def test_crawler_date_same_node() -> None:
    """Test date extraction when date is in the same text node as label."""
    crawler = Crawler()
    # Ensure container class="result" so it is found
    html = """
    <div class="result">
        <div>EudraCT Number: 2023-888</div>
        <div>Date record first entered: 2023-11-11</div>
    </div>
    """
    ids = crawler.extract_ids(html)
    assert len(ids) == 1
    assert ids[0][0] == "2023-888"
    assert str(ids[0][1]) == "2023-11-11"


def test_crawler_date_label_orphaned() -> None:
    """Test coverage for date label without parent."""
    crawler = Crawler()

    # We mock BeautifulSoup to return a NavigableString with no parent
    with patch("coreason_etl_euctr.crawler.BeautifulSoup") as MockSoup:
        mock_soup = MagicMock()
        MockSoup.return_value = mock_soup

        # We need to mock find_all to return ID label (so we enter loop)
        mock_id_label = MagicMock()  # No spec
        mock_id_label.parent.get_text.return_value = "EudraCT Number: 123"
        mock_id_label.parent.find_parent.return_value = MagicMock()  # Container

        # Mock container find to return date label
        mock_container = mock_id_label.parent.find_parent.return_value

        mock_date_label = MagicMock()
        mock_date_label.parent = None  # ORPHANED

        # The logic does: target = container.find(...)
        mock_container.find.return_value = mock_date_label

        mock_soup.find_all.return_value = [mock_id_label]

        # Run
        crawler.extract_ids("<html></html>")
        # Should not crash, date should be None
