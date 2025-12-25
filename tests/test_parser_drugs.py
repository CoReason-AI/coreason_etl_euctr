# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import pytest
from coreason_etl_euctr.parser import Parser


def test_parse_drugs_multiple() -> None:
    """Test parsing multiple drugs from a single HTML."""
    html = """
    <div>
        <table>
            <tr><td colspan="2">D. IMP IDENTIFICATION</td></tr>
            <tr>
                <td>D.2.1.1.1 Trade name:</td>
                <td>Drug A</td>
            </tr>
            <tr>
                <td>Name of Active Substance:</td>
                <td>Ingredient A</td>
            </tr>
            <tr>
                <td>Pharmaceutical form:</td>
                <td>Tablet</td>
            </tr>
        </table>

        <table>
            <tr><td colspan="2">D. IMP IDENTIFICATION</td></tr>
             <tr>
                <td>D.2.1.1.1 Trade name:</td>
                <td>Drug B</td>
            </tr>
            <tr>
                <td>Active Substance:</td>
                <td>Ingredient B</td>
            </tr>
            <tr>
                <td>CAS Number:</td>
                <td>123-45-6</td>
            </tr>
        </table>
    </div>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")

    assert len(drugs) == 2

    # Drug 1
    d1 = drugs[0]
    assert d1.drug_name == "Drug A"
    assert d1.active_ingredient == "Ingredient A"
    assert d1.pharmaceutical_form == "Tablet"
    assert d1.cas_number is None

    # Drug 2
    d2 = drugs[1]
    assert d2.drug_name == "Drug B"
    assert d2.active_ingredient == "Ingredient B"
    assert d2.cas_number == "123-45-6"


def test_parse_drugs_missing_fields() -> None:
    """Test parsing a drug with minimal fields."""
    html = """
    <table>
        <tr>
            <td>Name of Active Substance:</td>
            <td>Only Substance</td>
        </tr>
    </table>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")

    assert len(drugs) == 1
    d = drugs[0]
    assert d.drug_name is None
    assert d.active_ingredient == "Only Substance"


def test_parse_drugs_nested_structure() -> None:
    """Test parsing when labels are nested in b/span tags."""
    html = """
    <table>
        <tr>
            <td><b>Trade name:</b></td>
            <td><b>Drug Bold</b></td>
        </tr>
    </table>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")

    assert len(drugs) == 1
    assert drugs[0].drug_name == "Drug Bold"


def test_parse_drugs_empty_or_no_drugs() -> None:
    """Test parsing HTML with no drug sections."""
    html = "<html><body><p>No drugs here</p></body></html>"
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")
    assert drugs == []


def test_parse_drugs_malformed_html() -> None:
    """Test parsing malformed HTML does not crash."""
    html = "<table><tr><td>Trade name: Missing value</td>" # Unclosed tags
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")

    # It might find it depending on BS4 repair
    # BS4 usually closes tags. "Missing value" is in the same TD.
    # Logic looks for next sibling.
    # <tr><td>Trade Name: Missing Value</td></tr>
    # Text is "Trade Name: Missing Value".
    # _extract_field logic: find label "Trade Name". parent is td. next_sibling is None.
    # So it returns None.

    assert len(drugs) == 0


def test_parse_drugs_label_in_same_element() -> None:
    """
    Test when label and value are in the same element.
    The current _extract_field implementation logic relies on siblings.
    So it might return None if they are same element.
    This test verifies current behavior or need for improvement.
    """
    html = "<table><tr><td>Trade name: My Drug</td></tr></table>"
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")

    # Current implementation expects separate cell for value.
    # If this fails, we know we need to improve _extract_field or accepted limitation.
    # Based on EU CTR, it's usually table cells.
    # But let's see.
    assert len(drugs) == 0


def test_parse_drugs_false_positives() -> None:
    """Test that a table with just random text doesn't create a drug."""
    html = "<table><tr><td>Some other info</td></tr></table>"
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")
    assert len(drugs) == 0


def test_parse_drugs_product_name_alias() -> None:
    """Test finding 'Product Name' when 'Trade name' is absent."""
    html = """
    <table>
        <tr>
            <td>Product Name:</td>
            <td>My Product</td>
        </tr>
    </table>
    """
    parser = Parser()
    drugs = parser.parse_drugs(html, "2023-123")
    assert len(drugs) == 1
    assert drugs[0].drug_name == "My Product"
