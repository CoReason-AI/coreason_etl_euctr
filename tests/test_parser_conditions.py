# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from coreason_etl_euctr.parser import Parser


def test_parse_conditions_simple() -> None:
    html = """
    <table>
        <tr><td colspan="2">E. GENERAL INFORMATION ON THE TRIAL</td></tr>
        <tr>
            <td>E.1.1 Medical condition(s) being investigated:</td>
            <td>Cancer of the Lung</td>
        </tr>
        <tr>
            <td>E.1.2 MedDRA version:</td>
            <td>10.0</td>
        </tr>
    </table>
    """
    parser = Parser()

    conditions = parser.parse_conditions(html, "2023-123")

    assert len(conditions) == 1
    c = conditions[0]
    assert c.condition_name == "Cancer of the Lung"
    assert c.meddra_code is not None and "10.0" in c.meddra_code


def test_parse_conditions_multiple_blocks() -> None:
    """Test if multiple condition blocks exist (rare but possible in some formats)."""
    html = """
    <div>
        <table>
            <tr><td>E.1.1 Medical condition:</td><td>Flu</td></tr>
            <tr><td>E.1.2 MedDRA version:</td><td>v1</td></tr>
        </table>
        <table>
            <tr><td>E.1.1 Medical condition:</td><td>Cold</td></tr>
            <tr><td>E.1.2 MedDRA version:</td><td>v2</td></tr>
        </table>
    </div>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")

    assert len(conditions) == 2
    assert conditions[0].condition_name == "Flu"
    assert conditions[1].condition_name == "Cold"


def test_parse_conditions_missing_meddra() -> None:
    html = """
    <table>
        <tr>
            <td>E.1.1 Medical condition(s):</td>
            <td>Headache</td>
        </tr>
    </table>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")

    assert len(conditions) == 1
    assert conditions[0].condition_name == "Headache"
    assert conditions[0].meddra_code is None


def test_parse_conditions_empty() -> None:
    html = "<html></html>"
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")
    assert conditions == []

def test_parse_conditions_fallback_labels() -> None:
    """Test fallback to 'Medical condition(s) being investigated' if 'Medical condition' is missing."""
    html = """
    <table>
        <tr>
            <td>E.1.1 Medical condition(s) being investigated:</td>
            <td>Rare Disease</td>
        </tr>
    </table>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")
    assert len(conditions) == 1
    assert conditions[0].condition_name == "Rare Disease"

def test_parse_conditions_only_meddra_version() -> None:
    """Test extraction with only MedDRA version."""
    html = """
    <table>
        <tr>
            <td>E.1.2 MedDRA version:</td>
            <td>11.0</td>
        </tr>
    </table>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")
    assert len(conditions) == 1
    assert conditions[0].meddra_code == "11.0"

def test_parse_conditions_only_meddra_level() -> None:
    """Test extraction with only MedDRA level."""
    html = """
    <table>
        <tr>
            <td>E.1.2 MedDRA level:</td>
            <td>PT</td>
        </tr>
    </table>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")
    assert len(conditions) == 1
    assert conditions[0].meddra_code == "PT"

def test_parse_conditions_meddra_combined() -> None:
    """Test extraction with both MedDRA version and level."""
    html = """
    <table>
        <tr>
            <td>MedDRA version:</td>
            <td>12.0</td>
        </tr>
        <tr>
            <td>MedDRA level:</td>
            <td>LLT</td>
        </tr>
    </table>
    """
    parser = Parser()
    conditions = parser.parse_conditions(html, "2023-123")
    assert len(conditions) == 1
    assert conditions[0].meddra_code == "12.0 / LLT"
