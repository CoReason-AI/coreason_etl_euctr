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

import pytest
from coreason_etl_euctr.parser import Parser
from coreason_etl_euctr.utils import clean_text, is_retryable_error
from httpx import NetworkError


def test_clean_text() -> None:
    """Test text cleaning utility."""
    assert clean_text("  Hello   World  ") == "Hello World"
    assert clean_text("Hello\xa0World") == "Hello World"
    assert clean_text(None) == ""  # type: ignore


def test_is_retryable_error() -> None:
    """Test retryable error predicate."""
    assert is_retryable_error(NetworkError("fail")) is True
    assert is_retryable_error(ValueError("fail")) is False


def test_parse_trial_full() -> None:
    """Test parsing a complete trial HTML."""
    html = """
    <table>
        <tr>
            <td>EudraCT Number:</td> <td>2015-001234-56</td>
        </tr>
        <tr>
            <td>Name of Sponsor:</td> <td>Big Pharma Inc.</td>
        </tr>
        <tr>
            <td>Full title of the trial:</td> <td>A Big Trial</td>
        </tr>
        <tr>
            <td>Date of Competent Authority Decision:</td> <td>2023-01-01</td>
        </tr>
        <tr>
            <td>Trial Status:</td> <td>Completed</td>
        </tr>
    </table>
    <!-- Section F -->
    <table>
        <tr><td>F.1.1</td><td>Adults (18-64 years)</td><td>Yes</td></tr>
        <tr><td>F.1.2</td><td>Children</td><td>No</td></tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "http://source")

    assert trial.eudract_number == "2015-001234-56"
    assert trial.sponsor_name == "Big Pharma Inc."
    assert trial.trial_title == "A Big Trial"
    assert trial.start_date == date(2023, 1, 1)
    assert trial.trial_status == "Completed"
    assert trial.age_groups == ["Adults"]
    assert trial.url_source == "http://source"


def test_parse_trial_missing_id() -> None:
    """Test error when ID is missing."""
    html = "<html>Empty</html>"
    parser = Parser()
    with pytest.raises(ValueError, match="Could not extract EudraCT Number"):
        parser.parse_trial(html, "http://source")


def test_parse_trial_date_fallback() -> None:
    """Test date parsing fallback logic."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>123</td></tr>
        <tr><td>Date record first entered:</td><td>01/02/2023</td></tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "s")
    assert trial.start_date == date(2023, 2, 1)


def test_parse_trial_bad_date() -> None:
    """Test bad date raises error."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>123</td></tr>
        <tr><td>Date record first entered:</td><td>BadDate</td></tr>
    </table>
    """
    parser = Parser()
    with pytest.raises(ValueError, match="Could not parse date"):
        parser.parse_trial(html, "s")


def test_parse_drugs() -> None:
    """Test drug parsing."""
    html = """
    <table>
        <tr><td>Trade name:</td><td>WonderDrug</td></tr>
        <tr><td>Name of Active Substance:</td><td>Wonderium</td></tr>
        <tr><td>Pharmaceutical form:</td><td>Tablet</td></tr>
        <tr><td>CAS Number:</td><td>123-45-6</td></tr>
    </table>
    <table>
        <!-- Irrelevant table -->
        <tr><td>Foo</td><td>Bar</td></tr>
    </table>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "123")

    assert len(drugs) == 1
    d = drugs[0]
    assert d.eudract_number == "123"
    assert d.drug_name == "WonderDrug"
    assert d.active_ingredient == "Wonderium"
    assert d.pharmaceutical_form == "Tablet"
    assert d.cas_number == "123-45-6"


def test_parse_drugs_multiple() -> None:
    """Test parsing multiple drugs."""
    html = """
    <div>
        <table>
            <tr><td>Trade name:</td><td>Drug A</td></tr>
            <tr><td>Active Substance:</td><td>Sub A</td></tr>
            <tr><td>Pharmaceutical form:</td><td>Pill</td></tr>
            <tr><td>CAS Number:</td><td>1</td></tr>
        </table>
        <table>
            <tr><td>Product Name:</td><td>Drug B</td></tr>
            <tr><td>Name of Active Substance:</td><td>Sub B</td></tr>
            <tr><td>Pharmaceutical form:</td><td>Shot</td></tr>
            <tr><td>CAS Number:</td><td>2</td></tr>
        </table>
    </div>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "123")
    assert len(drugs) == 2
    assert drugs[0].drug_name == "Drug A"
    assert drugs[1].drug_name == "Drug B"


def test_parse_conditions() -> None:
    """Test condition parsing."""
    html = """
    <table>
        <tr><td>Medical condition:</td><td>Flu</td></tr>
        <tr><td>MedDRA version:</td><td>10.0</td></tr>
        <tr><td>MedDRA level:</td><td>PT</td></tr>
    </table>
    """
    parser = Parser()
    conds = parser.parse_conditions(html, "123")
    assert len(conds) == 1
    c = conds[0]
    assert c.condition_name == "Flu"
    assert c.meddra_code == "10.0 / PT"


def test_parse_conditions_alt_label() -> None:
    """Test condition parsing with alternative label."""
    html = """
    <table>
        <tr><td>Medical condition(s) being investigated:</td><td>Cold</td></tr>
    </table>
    """
    parser = Parser()
    conds = parser.parse_conditions(html, "123")
    assert len(conds) == 1
    assert conds[0].condition_name == "Cold"
    assert conds[0].meddra_code is None


def test_age_groups_parsing() -> None:
    """Test detailed age group parsing logic."""
    html = """
    <table>
        <tr><td>F.1.1</td><td>Adults (18-64 years)</td><td>Yes</td></tr>
        <tr><td>F.1.2</td><td>Children (2-11 years)</td><td></td></tr>
        <tr><td>F.1.3</td><td>Elderly (>=65 years)</td><td> Yes </td></tr>
    </table>
    """
    parser = Parser()
    # Access private method or use parse_trial
    # Using parse_trial for public interface testing
    html_wrapper = f"<table><tr><td>EudraCT Number:</td><td>1</td></tr></table>{html}"
    trial = parser.parse_trial(html_wrapper, "s")
    assert trial.age_groups == ["Adults", "Elderly"]


def test_extract_field_variants() -> None:
    """Test extraction logic robustness (bold, span, th)."""
    html = """
    <table>
        <tr>
            <th><b>Full title of the trial:</b></th>
            <td>The Title</td>
        </tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(f"<table><tr><td>EudraCT Number:</td><td>1</td></tr></table>{html}", "s")
    assert trial.trial_title == "The Title"

def test_parse_german_date() -> None:
    """Test DD.MM.YYYY parsing."""
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>1</td></tr>
        <tr><td>Date record first entered:</td><td>31.12.2023</td></tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "s")
    assert trial.start_date == date(2023, 12, 31)

def test_missing_title_fallback() -> None:
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>1</td></tr>
        <tr><td>Title of the trial for lay people:</td><td>Lay Title</td></tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "s")
    assert trial.trial_title == "Lay Title"

def test_status_fallback() -> None:
    html = """
    <table>
        <tr><td>EudraCT Number:</td><td>1</td></tr>
        <tr><td>Status of the trial:</td><td>Ongoing</td></tr>
    </table>
    """
    parser = Parser()
    trial = parser.parse_trial(html, "s")
    assert trial.trial_status == "Ongoing"

def test_parse_conditions_meddra_partials() -> None:
    """Test partial MedDRA codes."""
    html_v = "<table><tr><td>MedDRA version:</td><td>10.0</td></tr></table>"
    parser = Parser()
    conds_v = parser.parse_conditions(html_v, "1")
    assert conds_v[0].meddra_code == "10.0"

    html_l = "<table><tr><td>MedDRA level:</td><td>PT</td></tr></table>"
    conds_l = parser.parse_conditions(html_l, "1")
    assert conds_l[0].meddra_code == "PT"

def test_age_groups_edge_cases() -> None:
    """Test edge cases for age groups."""
    html = """
    <table>
        <!-- Row check -->
        <div>Bad Structure</div>
        <!-- No cells -->
        <tr></tr>
        <!-- Not enough cells -->
        <tr><td>F.1.1</td></tr>
        <!-- Match by Name in Label -->
        <tr><td>Label</td><td>Adults (18-64 years)</td><td>Yes</td></tr>
        <!-- Match by Name startswith -->
        <tr><td>Label</td><td>Infants and toddlers</td><td>Yes</td></tr>
    </table>
    """
    parser = Parser()
    html_wrapper = f"<table><tr><td>EudraCT Number:</td><td>1</td></tr></table>{html}"
    trial = parser.parse_trial(html_wrapper, "s")
    # Expect Adults (from Name) and Infants (from Name)
    assert "Adults" in str(trial.age_groups)
    assert "Infants and toddlers" in str(trial.age_groups)

def test_parse_drugs_dedup_tables() -> None:
    """Test that we don't process the same table twice if multiple markers match."""
    # This covers seen_ids logic
    html = """
    <table>
        <tr><td>Trade name:</td><td>Drug A</td></tr>
        <tr><td>Product Name:</td><td>Drug A (Again)</td></tr> <!-- Same table, different marker -->
    </table>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "1")
    assert len(drugs) == 1

def test_parse_empty_tables() -> None:
    """Test tables that match markers but contain no data."""
    # Condition table with no data
    html_cond = "<table><tr><td>MedDRA version:</td><td></td></tr></table>"
    parser = Parser()
    conds = parser.parse_conditions(html_cond, "1")
    assert len(conds) == 0

    # Drug table with no data
    html_drug = "<table><tr><td>Trade name:</td><td></td></tr></table>"
    drugs = parser.parse_drugs(html_drug, "1")
    assert len(drugs) == 0

def test_age_groups_malformed_html() -> None:
    """Test age group extraction with weird HTML structures."""
    # Covers defensive checks
    html = """
    <table>
        <!-- Yes text without cell -->
        Yes
        <!-- Yes in cell without row -->
        <td>Yes</td>
        <!-- Yes in row with not enough cells -->
        <tr><td>Yes</td></tr>
    </table>
    """
    parser = Parser()
    html_wrapper = f"<table><tr><td>EudraCT Number:</td><td>1</td></tr></table>{html}"
    trial = parser.parse_trial(html_wrapper, "s")
    assert trial.age_groups is None or len(trial.age_groups) == 0
