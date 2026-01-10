# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import unittest

from bs4 import BeautifulSoup

from coreason_etl_euctr.parser import Parser


class TestParserAgeGroups(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = Parser()

    def test_parse_age_groups_adults_only(self) -> None:
        html = """
        <html>
        <body>
            <table>
                <tr>
                    <td>F.1.1</td>
                    <td>Adults (18-64 years)</td>
                    <td>Yes</td>
                </tr>
                <tr>
                    <td>F.1.2</td>
                    <td>Children (2-11 years)</td>
                    <td>No</td>
                </tr>
            </table>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults"])

    def test_parse_age_groups_mixed(self) -> None:
        html = """
        <table>
            <tr><td>F.1.1</td><td>Adults</td><td>Yes</td></tr>
            <tr><td>F.1.2.1</td><td>Preterm newborn infants</td><td>Yes</td></tr>
            <tr><td>F.1.3</td><td>Elderly</td><td>No</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults", "Preterm newborn infants"])

    def test_parse_age_groups_none(self) -> None:
        html = "<html><body><p>No tables here</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertIsNone(groups)

    def test_parse_age_groups_case_insensitive(self) -> None:
        html = """
        <table>
            <tr><td>F.1.3</td><td>Elderly</td><td>YES</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Elderly"])

    def test_parse_age_groups_edge_cases_structure(self) -> None:
        """Test malformed HTML structures around 'Yes'."""
        html = """
        <div>Yes</div>
        <table>
             <tr><td>Yes</td></tr> <!-- Single cell row -->
             <tr><td></td><td></td><td>Other</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        # Add orphan td manually
        orphan_td = soup.new_tag("td")
        orphan_td.string = "Yes"
        soup.append(orphan_td)

        groups = self.parser._parse_age_groups(soup)
        self.assertIsNone(groups)

    def test_parse_age_groups_fallback_label(self) -> None:
        """Test fallback to first cell if second is empty."""
        # Compact HTML to ensure no parsing ambiguity
        # 3 cells. Middle is empty.
        html = "<table><tr><td>Adults</td><td></td><td>Yes</td></tr></table>"
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults"])

    def test_parse_age_groups_two_cells(self) -> None:
        """Test row with exactly 2 cells (Label, Value)."""
        html = "<table><tr><td>Adults</td><td>Yes</td></tr></table>"
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults"])

    def test_parse_age_groups_no_match(self) -> None:
        """Test row has 'Yes' but text doesn't match any known age group."""
        html = """
        <table>
            <tr><td>G.1.1</td><td>Something else</td><td>Yes</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertIsNone(groups)
