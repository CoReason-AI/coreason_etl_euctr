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
from typing import List, Optional, Union

from bs4 import BeautifulSoup, Tag
from loguru import logger

from coreason_etl_euctr.models import EuTrial, EuTrialDrug


class Parser:
    """
    Parser module responsible for transforming raw HTML into Silver layer Pydantic models.
    """

    def parse_trial(self, html_content: str, url_source: str) -> EuTrial:
        """
        Parse the core trial details from the HTML content.

        Args:
            html_content: The raw HTML string.
            url_source: The source URL (required for the model).

        Returns:
            EuTrial: The populated Pydantic model.
        """
        soup = BeautifulSoup(html_content, "html.parser")

        # 1. EudraCT Number (Header)
        # Usually found in a table with label "EudraCT Number:"
        eudract_number = self._extract_field(soup, "EudraCT Number")
        if not eudract_number:
            # Fallback: try to find it in the title or H1?
            # For now, if we can't find the ID, it's a critical failure for this record.
            # But we might have it from the filename/url.
            # The FRD says "EudraCT Number: Source of Truth ID".
            # Let's assume it's in the body. If not, raise or return partial?
            # Model says it's required.
            # We'll try to extract. If fail, maybe rely on caller?
            # Ideally the parser extracts what is IN the file.
            raise ValueError("Could not extract EudraCT Number from HTML.")

        # 2. Sponsor Name (Section B)
        # Label: "Name of Sponsor" (B.1.1)
        sponsor_name = self._extract_field(soup, "Name of Sponsor")

        # 3. Trial Title (Section A)
        # Label: "Full title of the trial" (A.3.1)
        trial_title = self._extract_field(soup, "Full title of the trial")
        if not trial_title:
            # Fallback to "Title of the trial for lay people"
            trial_title = self._extract_field(soup, "Title of the trial for lay people")

        # 4. Start Date / Date of Decision
        # Label: "Date of Competent Authority Decision" OR "Date record first entered"
        start_date = self._parse_date(soup)

        # 5. Trial Status
        # Label: "Trial Status" -> often "Global end of the trial" or specific country status
        # In EU CTR, status is often per country.
        # We look for "Trial Status" in the header table or "Status of the trial"
        trial_status = self._extract_field(soup, "Trial Status")
        if not trial_status:
            trial_status = self._extract_field(soup, "Status of the trial")

        return EuTrial(
            eudract_number=eudract_number,
            sponsor_name=sponsor_name,
            trial_title=trial_title,
            start_date=start_date,
            trial_status=trial_status,
            url_source=url_source,
        )

    def parse_drugs(self, html_content: str, eudract_number: str) -> List[EuTrialDrug]:
        """
        Parse Section D to extract one or more drugs.

        Args:
            html_content: The raw HTML string.
            eudract_number: The trial ID to link these drugs to.

        Returns:
            List[EuTrialDrug]: A list of extracted drugs.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        drugs: List[EuTrialDrug] = []

        # Strategy: Find all tables that contain drug-specific labels.
        # This handles multiple drugs (one table per drug) and robustly finds them
        # without relying on fixed section IDs.
        # Labels: "Trade name", "Name of Active Substance", "Pharmaceutical form"
        target_labels = [
            "Trade name",
            "Name of Active Substance",
            "Product Name",  # sometimes used instead of Trade name
            "Pharmaceutical form",
        ]

        # Use a regex that matches any of the target labels
        pattern = re.compile(
            r"(" + "|".join([re.escape(label) for label in target_labels]) + r")", re.IGNORECASE
        )

        # Find all markers
        markers = soup.find_all(string=pattern)

        # Collect unique parent tables maintaining order
        candidate_tables = []
        seen_ids = set()

        for marker in markers:
            # Find closest parent table
            parent_table = marker.find_parent("table")
            if parent_table and id(parent_table) not in seen_ids:
                # Double check if this table is likely a Drug table (Section D)
                # Or at least not the main container if possible.
                # Usually checking if it contains "Section D" or "IMP" helps,
                # but sometimes the "Section D" header is outside this table (sibling).
                # For robustness, we process any table having these specific fields.
                candidate_tables.append(parent_table)
                seen_ids.add(id(parent_table))

        for tbl in candidate_tables:
            drug = self._parse_single_drug(tbl, eudract_number)
            if drug:
                drugs.append(drug)

        return drugs

    def _parse_single_drug(self, soup: Union[BeautifulSoup, Tag], eudract_number: str) -> Optional[EuTrialDrug]:
        """
        Extract drug fields from a specific table/section.
        Returns None if no relevant data found (false positive table).
        """
        drug_name = self._extract_field(soup, "Trade name")
        if not drug_name:
            drug_name = self._extract_field(soup, "Product Name")

        active_ingredient = self._extract_field(soup, "Name of Active Substance")
        if not active_ingredient:
            active_ingredient = self._extract_field(soup, "Active Substance")

        pharm_form = self._extract_field(soup, "Pharmaceutical form")

        cas_number = self._extract_field(soup, "CAS Number")

        # If we found nothing relevant, it's likely a false positive table (e.g. a summary)
        if not any([drug_name, active_ingredient, pharm_form, cas_number]):
            return None

        return EuTrialDrug(
            eudract_number=eudract_number,
            drug_name=drug_name,
            active_ingredient=active_ingredient,
            cas_number=cas_number,
            pharmaceutical_form=pharm_form,
        )

    def _extract_field(self, soup: Union[BeautifulSoup, Tag], label_text: str) -> Optional[str]:
        """
        Helper to find a field by its label.
        Handles the messy table structure of EU CTR.
        """
        # Find the label. Use a regex to be flexible with colons and whitespace
        label_pattern = re.compile(rf"{re.escape(label_text)}\s*:?", re.IGNORECASE)
        target = soup.find(string=label_pattern)

        if not target:
            return None

        # Logic:
        # 1. Check parent's siblings (td -> td)
        # 2. Check parent's parent's siblings (tr -> tr, if label is in a th)
        # The structure is often: <tr> <td class="label">Label:</td> <td class="value">Value</td> </tr>

        parent = target.parent
        if not parent:
            return None

        # Try next sibling of the parent element
        next_sibling = parent.find_next_sibling()

        # If the label is inside a <b> or <span> inside a <td>, we might need to go up one level
        if next_sibling is None and parent.name in ["b", "span", "strong"]:
            parent = parent.parent
            if parent:
                next_sibling = parent.find_next_sibling()

        if next_sibling:
            text = next_sibling.get_text(strip=True)
            if text:
                return self._clean_text(text)

        # Sometimes the value is in the same element?
        # "EudraCT Number: 2004..." -> already handled by the get_text logic if we searched string
        # But if we searched string, `target` is the NavigableString.
        # `parent` is the element containing it.
        # full_text = parent.get_text(" ", strip=True)
        # If the label is part of the text, remove it.
        # But this is risky if the layout is tabular.

        return None

    def _clean_text(self, text: str) -> str:
        """Remove excessive whitespace and non-breaking spaces."""
        # Replace non-breaking space
        text = text.replace("\xa0", " ")
        # Collapse whitespace
        text = " ".join(text.split())
        return text

    def _parse_date(self, soup: BeautifulSoup) -> Optional[date]:
        """
        Attempt to parse the start date.
        Priority:
        1. Date of Competent Authority Decision
        2. Date record first entered
        """
        date_str = self._extract_field(soup, "Date of Competent Authority Decision")
        if not date_str:
            date_str = self._extract_field(soup, "Date record first entered")

        if not date_str:
            return None

        # Formats: YYYY-MM-DD or DD/MM/YYYY
        # EU CTR usually uses YYYY-MM-DD but let's be robust
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            pass

        logger.warning(f"Could not parse date: {date_str}")
        return None
