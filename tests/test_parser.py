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


@pytest.fixture
def parser() -> Parser:
    return Parser()


def test_parse_simple_trial(parser: Parser) -> None:
    html = """
    <html>
        <table>
            <tr><td>EudraCT Number:</td><td>2021-123456-78</td></tr>
            <tr><td>Full title of the trial:</td><td>Test Study for X</td></tr>
            <tr><td>Name of Sponsor:</td><td>Big Pharma</td></tr>
            <tr><td>Date of Competent Authority Decision:</td><td>2021-01-01</td></tr>
            <tr><td>Trial status:</td><td>Completed</td></tr>
        </table>
    </html>
    """
    trial = parser.parse_file(html, source_url="http://test")

    assert trial.eudract_number == "2021-123456-78"
    assert trial.sponsor_name == "Big Pharma"
    assert trial.trial_title == "Test Study for X"
    assert trial.start_date == date(2021, 1, 1)
    assert trial.trial_status == "Completed"
    assert trial.url_source == "http://test"
    assert trial.drugs == []
    assert trial.conditions == []


def test_parse_drugs_section(parser: Parser) -> None:
    html = """
    <html>
        <table><tr><td>EudraCT Number:</td><td>2021-000</td></tr></table>
        <!-- Drug 1 -->
        <table>
            <tr><td>D. IMP Identification</td></tr>
            <tr><td>D.2.1.1.1 Trade name</td><td>SuperPill</td></tr>
            <tr><td>D.3.8 Name of Active Substance</td><td>MagicDust</td></tr>
            <tr><td>Pharmaceutical form</td><td>Tablet</td></tr>
        </table>
        <!-- Drug 2 -->
        <table>
            <tr><td>D. IMP Identification</td></tr>
            <tr><td>Trade name</td><td>Placebo</td></tr>
            <tr><td>Product Name</td><td>SugarPill</td></tr>
            <tr><td>Pharmaceutical form</td><td>Capsule</td></tr>
        </table>
    </html>
    """
    trial = parser.parse_file(html)

    assert len(trial.drugs) == 2

    # Sort or find by name to verify specific fields
    d1 = next(d for d in trial.drugs if d.drug_name == "SuperPill")
    assert d1.active_ingredient == "MagicDust"
    assert d1.pharmaceutical_form == "Tablet"

    d2 = next(d for d in trial.drugs if d.drug_name == "Placebo")
    assert d2.active_ingredient == "SugarPill"  # Fallback to Product Name
    assert d2.pharmaceutical_form == "Capsule"


def test_parse_conditions_section(parser: Parser) -> None:
    html = """
    <html>
        <table><tr><td>EudraCT Number:</td><td>2021-000</td></tr></table>
        <table>
            <tr><td>E.1.1 Medical condition(s) being investigated</td><td>Chronic Fatigue</td></tr>
            <tr><td>E.1.2 MedDRA version</td><td>23.0</td></tr>
        </table>
    </html>
    """
    trial = parser.parse_file(html)

    assert len(trial.conditions) == 1
    assert trial.conditions[0].condition_name == "Chronic Fatigue"
    assert trial.conditions[0].meddra_code == "23.0"


def test_missing_eudract_raises_error(parser: Parser) -> None:
    html = "<html><body>No ID here</body></html>"
    with pytest.raises(ValueError, match="Could not find EudraCT Number"):
        parser.parse_file(html)


def test_date_formats(parser: Parser) -> None:
    html_template = """
    <html><table>
        <tr><td>EudraCT Number:</td><td>2021-000</td></tr>
        <tr><td>Date record first entered:</td><td>{}</td></tr>
    </table></html>
    """

    # ISO Format
    trial = parser.parse_file(html_template.format("2022-12-31"))
    assert trial.start_date == date(2022, 12, 31)

    # Slash Format
    trial = parser.parse_file(html_template.format("31/12/2022"))
    assert trial.start_date == date(2022, 12, 31)

    # Invalid
    trial = parser.parse_file(html_template.format("Not a date"))
    assert trial.start_date is None


def test_clean_text_utility(parser: Parser) -> None:
    html = """
    <html><table>
        <tr><td>EudraCT Number:</td><td>  2021-000&nbsp; </td></tr>
    </table></html>
    """
    trial = parser.parse_file(html)
    assert trial.eudract_number == "2021-000"
