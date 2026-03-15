# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr


from coreason_etl_euctr.parser import EpistemicParserTask


def test_basic_anchor_extraction() -> None:
    html = """
    <table>
        <tr><td class="first">A.2</td><td class="second">EudraCT Number</td>
            <td class="third">2020-000000-00</td></tr>
        <tr><td class="first">E.3</td><td class="second">Principal Inclusion Criteria</td>
            <td class="third">Patients over 18.</td></tr>
    </table>
    """
    parser = EpistemicParserTask()
    parsed_data = parser.parse_html(html)

    assert parsed_data["A.2"] == "2020-000000-00"
    assert parsed_data["E.3"] == "Patients over 18."


def test_hierarchical_block_grouping() -> None:
    html = """
    <table>
        <tr><td class="first">A.2</td><td class="second">EudraCT Number</td>
            <td class="third">2020-000000-00</td></tr>
        <tr><td colspan="3" class="cellBlue">D.IMP: 1</td></tr>
        <tr><td class="first">D.2.1</td><td class="second">Trade Name</td>
            <td class="third">Drug A</td></tr>
        <tr><td colspan="3" class="cellBlue">D.IMP: 2</td></tr>
        <tr><td class="first">D.2.1</td><td class="second">Trade Name</td>
            <td class="third">Drug B</td></tr>
        <tr><td class="first">D.3.1</td><td class="second">Product Name</td>
            <td class="third">Test B</td></tr>
    </table>
    """
    parser = EpistemicParserTask()
    parsed_data = parser.parse_html(html)

    assert parsed_data["A.2"] == "2020-000000-00"
    assert isinstance(parsed_data["D.IMP"], list)
    assert len(parsed_data["D.IMP"]) == 2

    assert parsed_data["D.IMP"][0]["D.2.1"] == "Drug A"
    assert parsed_data["D.IMP"][1]["D.2.1"] == "Drug B"
    assert parsed_data["D.IMP"][1]["D.3.1"] == "Test B"


def test_missing_keys_skipped() -> None:
    html = """
    <table>
        <tr><td class="first"></td><td class="second">No Key</td><td class="third">Skipped</td></tr>
        <tr><td class="first">A.3</td><td class="second">Title</td><td class="third">Some Title</td></tr>
    </table>
    """
    parser = EpistemicParserTask()
    parsed_data = parser.parse_html(html)

    assert "A.3" in parsed_data
    assert parsed_data["A.3"] == "Some Title"
    assert "" not in parsed_data


def test_reset_block_grouping() -> None:
    html = """
    <table>
        <tr><td colspan="3" class="cellBlue">D.IMP: 1</td></tr>
        <tr><td class="first">D.2.1</td><td class="second">Trade Name</td>
            <td class="third">Drug A</td></tr>
        <tr><td class="cellBlue">General Header Not Block</td></tr>
        <tr><td class="first">E.1</td><td class="second">Another Area</td>
            <td class="third">Values</td></tr>
    </table>
    """
    parser = EpistemicParserTask()
    parsed_data = parser.parse_html(html)

    assert len(parsed_data["D.IMP"]) == 1
    assert parsed_data["D.IMP"][0]["D.2.1"] == "Drug A"
    assert parsed_data["E.1"] == "Values"


def test_multi_line_content() -> None:
    html = """
    <table>
        <tr><td class="first">F.1</td><td class="second">Desc</td><td class="third">Line 1<br/>Line 2</td></tr>
    </table>
    """
    parser = EpistemicParserTask()
    parsed_data = parser.parse_html(html)

    assert parsed_data["F.1"] == "Line 1 Line 2"
