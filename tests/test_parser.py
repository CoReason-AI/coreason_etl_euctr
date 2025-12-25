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
from coreason_etl_euctr.parser import Parser


def test_parse_trial_success() -> None:
    """Test successful extraction of a standard trial."""
    html = """
    <table>
        <tr>
            <td class="label">EudraCT Number:</td>
            <td class="value">2023-123456-78</td>
        </tr>
        <tr>
            <td class="label">Name of Sponsor:</td>
            <td class="value">  Acme  Pharma  Inc.  </td>
        </tr>
        <tr>
            <td class="label">Full title of the trial:</td>
            <td class="value">A Randomized Control Trial</td>
        </tr>
        <tr>
            <td class="label">Date of Competent Authority Decision:</td>
            <td class="value">2023-05-20</td>
        </tr>
        <tr>
            <td class="label">Trial Status:</td>
            <td class="value">Ongoing</td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source.url")

    assert trial.eudract_number == "2023-123456-78"
    assert trial.sponsor_name == "Acme Pharma Inc."
    assert trial.trial_title == "A Randomized Control Trial"
    assert trial.start_date == date(2023, 5, 20)
    assert trial.trial_status == "Ongoing"
    assert trial.url_source == "http://source.url"


def test_parse_trial_missing_id() -> None:
    """Test that missing EudraCT Number raises ValueError."""
    html = "<html><body>No ID here</body></html>"
    parser = Parser()
    with pytest.raises(ValueError, match="Could not extract EudraCT Number"):
        parser.parse_trial(html, "http://source.url")


def test_parse_trial_fallback_fields() -> None:
    """Test fallback logic for Title and Date."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>2023-123</td></tr>
        <tr>
            <td>Title of the trial for lay people:</td>
            <td>Fallback Title</td>
        </tr>
        <tr>
            <td>Date record first entered:</td>
            <td>20/05/2023</td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source.url")

    assert trial.trial_title == "Fallback Title"
    assert trial.start_date == date(2023, 5, 20)


def test_parse_trial_nested_structure() -> None:
    """Test extraction when labels are inside bold tags (common in older HTML)."""
    html = """
    <table>
        <tr>
            <td><b>EudraCT Number:</b></td>
            <td>2023-123</td>
        </tr>
        <tr>
            <td><span>Name of Sponsor:</span></td>
            <td>Acme</td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source.url")

    assert trial.eudract_number == "2023-123"
    assert trial.sponsor_name == "Acme"


def test_parse_trial_invalid_date() -> None:
    """Test graceful handling of invalid dates."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>2023-123</td></tr>
        <tr>
            <td>Date of Competent Authority Decision:</td>
            <td>Not a Date</td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source.url")

    assert trial.eudract_number == "2023-123"
    assert trial.start_date is None


def test_parse_trial_whitespace_cleaning() -> None:
    """Test that non-breaking spaces and newlines are cleaned."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>2023-123</td></tr>
        <tr>
            <td>Name of Sponsor:</td>
            <td>  Mega  \n\xa0  Corp  </td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source.url")
    assert trial.sponsor_name == "Mega Corp"


def test_extract_field_orphaned_label() -> None:
    """Test _extract_field with an orphaned string (no parent)."""
    parser = Parser()
    # Create a soup with a string that has no parent (mocking or modifying soup)
    from bs4 import NavigableString

    # Mocking soup to return an orphaned string
    mock_soup = MagicMock()
    mock_string = NavigableString("EudraCT Number:")
    # By default NavigableString parent might be None if not attached

    mock_soup.find.return_value = mock_string

    # We need to access the private method to test this specific edge case easily
    # or rely on parse_trial failing.
    # Let's test _extract_field directly if possible or mock the internal find.

    res = parser._extract_field(mock_soup, "EudraCT Number")
    assert res is None


def test_extract_field_nested_no_parent() -> None:
    """Test nested label where the parent element itself has no parent."""
    parser = Parser()
    mock_soup = MagicMock()

    # Simulate <b>Label</b> where <b> has no parent
    mock_tag = MagicMock(name="b")
    mock_tag.name = "b"
    mock_tag.parent = None
    # Crucial: Ensure find_next_sibling returns None (no sibling)
    mock_tag.find_next_sibling.return_value = None

    mock_string = MagicMock()
    mock_string.parent = mock_tag

    mock_soup.find.return_value = mock_string

    res = parser._extract_field(mock_soup, "Label")
    assert res is None
