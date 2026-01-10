# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date
from unittest.mock import MagicMock

import pytest
from bs4 import BeautifulSoup, NavigableString

from coreason_etl_euctr.utils import clean_text, extract_field_by_label, parse_flexible_date


def test_clean_text() -> None:
    assert clean_text("  Hello   World  ") == "Hello World"
    assert clean_text("Hello\xa0World") == "Hello World"
    assert clean_text(None) == ""  # type: ignore


def test_parse_flexible_date() -> None:
    assert parse_flexible_date("2023-01-01") == date(2023, 1, 1)
    assert parse_flexible_date("01/01/2023") == date(2023, 1, 1)
    assert parse_flexible_date("01.01.2023") == date(2023, 1, 1)
    with pytest.raises(ValueError):
        parse_flexible_date("Invalid")
    assert parse_flexible_date(None) is None


def test_extract_field_simple() -> None:
    html = """
    <table>
        <tr>
            <td>Label:</td>
            <td>Value</td>
        </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    assert extract_field_by_label(soup, "Label") == "Value"


def test_extract_field_nested_label() -> None:
    html = """
    <table>
        <tr>
            <td><b>Label:</b></td>
            <td>Value</td>
        </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    assert extract_field_by_label(soup, "Label") == "Value"


def test_extract_field_not_found() -> None:
    soup = BeautifulSoup("<html></html>", "html.parser")
    assert extract_field_by_label(soup, "Missing") is None


def test_extract_field_no_sibling() -> None:
    html = "<div>Label:</div>"
    soup = BeautifulSoup(html, "html.parser")
    assert extract_field_by_label(soup, "Label") is None


def test_extract_field_orphaned_label() -> None:
    """Test extract_field with an orphaned string (no parent)."""
    # Simulate a string that matches search but has no parent
    mock_soup = MagicMock()
    mock_string = NavigableString("Label:")
    # NavigableString by default has parent=None unless attached

    mock_soup.find.return_value = mock_string

    assert extract_field_by_label(mock_soup, "Label") is None


def test_extract_field_nested_no_parent() -> None:
    """Test nested label where the parent element itself has no parent."""
    mock_soup = MagicMock()

    # Simulate <b>Label</b> where <b> has no parent
    mock_tag = MagicMock(name="b")
    mock_tag.name = "b"
    mock_tag.parent = None
    mock_tag.find_next_sibling.return_value = None

    mock_string = MagicMock()
    mock_string.parent = mock_tag

    mock_soup.find.return_value = mock_string

    assert extract_field_by_label(mock_soup, "Label") is None


def test_extract_field_empty_value() -> None:
    """Test when sibling exists but text is empty/whitespace."""
    html = """
    <table>
        <tr>
            <td>Label:</td>
            <td>   </td>
        </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    # Should return None if text is empty after cleaning,
    # OR if our utils function returns None.
    # Utils code: if text: return clean_text(text).
    # get_text(strip=True) on "   " returns "".
    # if "" -> False. Returns None.
    assert extract_field_by_label(soup, "Label") is None


def test_extract_field_value_is_empty_string() -> None:
    """Explicitly test the branch where get_text returns empty string."""
    # We need a case where next_sibling exists but get_text(strip=True) is empty
    html = "<div><span>Label:</span><span>   </span></div>"
    soup = BeautifulSoup(html, "html.parser")
    assert extract_field_by_label(soup, "Label") is None
