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


class TestParserAgeGroupsComplex(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = Parser()

    def test_parse_age_groups_false_positive_context(self) -> None:
        """
        Test that the parser does not incorrectly identify an Age Group
        just because the word 'Adults' appears in a row with 'Yes'.
        """
        # Example: A question about informed consent involving adults
        html = """
        <table>
            <tr>
                <td>Is informed consent obtained from legal representatives for Adults?</td>
                <td>Yes</td>
            </tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)

        # Should be None or empty, because this is not Section F / Age Group table
        # Currently, the loose logic might match "Adults".
        # We expect the parser to be smart enough (or we will make it so).
        # For now, let's assert what we WANT: None.
        self.assertIsNone(groups)

    def test_parse_age_groups_unicode_and_whitespace(self) -> None:
        """Test with non-breaking spaces and Unicode."""
        html = """
        <table>
            <tr>
                <td>F.1.1</td>
                <td>Adults&nbsp;(18-64&nbsp;years)</td> <!-- HTML Entity nbsp -->
                <td>Yes</td>
            </tr>
            <tr>
                <td>F.1.3</td>
                <td>Elderly (≥ 65 years)</td> <!-- Unicode char -->
                <td>Yes</td>
            </tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        expected = sorted(["Adults", "Elderly"])
        self.assertEqual(groups, expected)

    def test_parse_age_groups_multiple_tables_split(self) -> None:
        """Test Age Groups scattered across multiple tables (e.g. bad formatting)."""
        html = """
        <table>
            <tr><td>F.1.1</td><td>Adults</td><td>Yes</td></tr>
        </table>
        <p>Separator</p>
        <table>
            <tr><td>F.1.2.1</td><td>Preterm newborn infants</td><td>Yes</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        expected = sorted(["Adults", "Preterm newborn infants"])
        self.assertEqual(groups, expected)

    def test_parse_age_groups_nested_structure(self) -> None:
        """Test tables nested inside divs or other tables."""
        html = """
        <div>
            <table>
                <tr>
                    <td>
                        <table>
                            <tr><td>F.1.1</td><td>Adults</td><td>Yes</td></tr>
                        </table>
                    </td>
                </tr>
            </table>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults"])

    def test_parse_age_groups_duplicates(self) -> None:
        """Test that duplicate groups are deduped."""
        html = """
        <table>
            <tr><td>F.1.1</td><td>Adults</td><td>Yes</td></tr>
            <!-- Duplicate entry -->
            <tr><td>F.1.1</td><td>Adults</td><td>Yes</td></tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        groups = self.parser._parse_age_groups(soup)
        self.assertEqual(groups, ["Adults"])
