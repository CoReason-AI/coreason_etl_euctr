# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

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
