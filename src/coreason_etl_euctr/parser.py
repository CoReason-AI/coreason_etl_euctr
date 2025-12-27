# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import re
from datetime import date, datetime
from typing import List, Optional

from bs4 import BeautifulSoup, Tag

from coreason_etl_euctr.models import EuTrial, EuTrialCondition, EuTrialDrug


class Parser:
    """
    Parses EudraCT HTML files into structured Pydantic models.
    """

    def parse_file(self, content: str, source_url: str = "") -> EuTrial:
        """
        Main entry point to parse a full HTML file.
        """
        soup = BeautifulSoup(content, "html.parser")

        # 1. Extract Core Trial Data
        eudract_number = self._extract_eudract_number(soup)
        if not eudract_number:
            raise ValueError("Could not find EudraCT Number in HTML.")

        trial = EuTrial(
            eudract_number=eudract_number,
            sponsor_name=self._extract_field_by_label(soup, "Name of Sponsor"),
            trial_title=self._extract_field_by_label(soup, "Full title of the trial"),
            start_date=self._extract_start_date(soup),
            trial_status=self._extract_field_by_label(soup, "Trial status"),
            url_source=source_url,
            drugs=self._parse_drugs(soup),
            conditions=self._parse_conditions(soup),
        )
        return trial

    def _extract_eudract_number(self, soup: BeautifulSoup) -> Optional[str]:
        # Usually in the header table: <td>EudraCT Number:</td><td>2004-000000-00</td>
        return self._extract_field_by_label(soup, "EudraCT Number")

    def _extract_start_date(self, soup: BeautifulSoup) -> Optional[date]:
        # Try "Date of Competent Authority Decision" or "Date record first entered"
        date_str = self._extract_field_by_label(soup, "Date of Competent Authority Decision")
        if not date_str:
            date_str = self._extract_field_by_label(soup, "Date record first entered")

        if date_str:
            return self._parse_flexible_date(date_str)
        return None

    def _parse_drugs(self, soup: BeautifulSoup) -> List[EuTrialDrug]:
        """
        Extracts drugs from Section D.
        Section D often repeats for multiple IMPs.
        """
        drugs = []
        # Find all tables or sections that look like IMP sections
        # Strategy: Search for "D. IMP Identification" headers or similar
        # Since HTML structure varies, we iterate through tables containing "Trade name"

        # Simplified: Find all tables containing "D. IMP Identification" or "D. IMP"
        # Then within those tables, extract fields.

        # Robust approach: Find all 'td' elements with "Trade name" and traverse up to the table
        seen_drugs = set()

        # Looking for D.2.1.1.1 Trade name
        # Using a set of search terms to find drug tables
        targets = soup.find_all(string=re.compile(r"Trade name"))
        for target in targets:
            # Go up to the table containing this row
            table = target.find_parent("table")
            if not table:  # pragma: no cover
                continue

            drug = self._parse_single_drug(table)
            # Dedup based on fields to avoid parsing same table twice if multiple hits
            drug_tuple = (drug.drug_name, drug.active_ingredient)
            if drug_tuple not in seen_drugs and (drug.drug_name or drug.active_ingredient):
                drugs.append(drug)
                seen_drugs.add(drug_tuple)

        return drugs

    def _parse_single_drug(self, table: Tag) -> EuTrialDrug:
        drug_name = self._extract_field_from_table(table, "Trade name")
        active_ingredient = self._extract_field_from_table(table, "Name of Active Substance")
        # Fallback for active ingredient
        if not active_ingredient:
            active_ingredient = self._extract_field_from_table(table, "Product Name")

        pharm_form = self._extract_field_from_table(table, "Pharmaceutical form")
        cas_number = self._extract_field_from_table(table, "CAS Number")

        return EuTrialDrug(
            drug_name=drug_name,
            active_ingredient=active_ingredient,
            pharmaceutical_form=pharm_form,
            cas_number=cas_number,
        )

    def _parse_conditions(self, soup: BeautifulSoup) -> List[EuTrialCondition]:
        """
        Extracts conditions from Section E.
        """
        conditions = []
        # Look for "E.1.1 Medical condition(s) being investigated"
        # This might be a single field with multiple lines or multiple tables

        # Primary strategy: Find the label and get the value
        condition_name = self._extract_field_by_label(soup, "Medical condition(s) being investigated")
        meddra_code = self._extract_field_by_label(soup, "MedDRA version")  # or code

        # Note: Often there is only one condition block per trial in the summary,
        # but if there are multiple, they might be in separate tables.
        # For this iteration, we extract the primary one found.
        # Enhancing for multiple conditions would require table iteration like drugs.

        if condition_name:
            # Sometimes multiple conditions are listed in one text block
            # For now, treat as one entry or split by newline?
            # FRD says "normalized One-to-Many", but source often has just text.
            # We will create one record per trial unless we see distinct tables.
            conditions.append(EuTrialCondition(condition_name=condition_name, meddra_code=meddra_code))

        return conditions

    def _extract_field_by_label(self, soup: BeautifulSoup, label_substring: str) -> Optional[str]:
        """
        Finds a table cell with the label and returns the next cell's text.
        """
        # Look for text node containing label
        target = soup.find(string=re.compile(re.escape(label_substring), re.IGNORECASE))
        if target:
            # Often structure is <td>Label:</td> <td>Value</td>
            # Or <tr><td>Label</td></tr><tr><td>Value</td></tr>
            # We try finding the parent TD or DT, then next sibling
            element = target.find_parent(["td", "dt", "th"])
            if element:
                next_element = element.find_next_sibling(["td", "dd"])
                if next_element:  # pragma: no cover
                    return self._clean_text(next_element.get_text())
        return None

    def _extract_field_from_table(self, table: Tag, label_substring: str) -> Optional[str]:
        """
        Scoped extraction within a specific table tag.
        """
        target = table.find(string=re.compile(re.escape(label_substring), re.IGNORECASE))
        if target:
            element = target.find_parent(["td", "dt", "th"])
            if element:
                next_element = element.find_next_sibling(["td", "dd"])
                if next_element:  # pragma: no cover
                    return self._clean_text(next_element.get_text())
        return None

    def _clean_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        # Remove non-breaking spaces and trim
        cleaned = text.replace("\xa0", " ").strip()
        # Remove "Attributes:" or other prefixes if common?
        return cleaned if cleaned else None  # pragma: no cover

    def _parse_flexible_date(self, date_str: str) -> Optional[date]:
        """
        Parses date strings like '2021-01-31' or '31/01/2021'.
        """
        date_str = date_str.strip()
        formats = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None
