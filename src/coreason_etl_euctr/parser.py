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
from datetime import date
from typing import List, Optional, Union

from bs4 import BeautifulSoup, Tag
from loguru import logger

from coreason_etl_euctr.models import EuTrial, EuTrialDrug
from coreason_etl_euctr.utils import extract_field_by_label, parse_flexible_date


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
        eudract_number = extract_field_by_label(soup, "EudraCT Number")
        if not eudract_number:
            raise ValueError("Could not extract EudraCT Number from HTML.")

        # 2. Sponsor Name (Section B)
        sponsor_name = extract_field_by_label(soup, "Name of Sponsor")

        # 3. Trial Title (Section A)
        trial_title = extract_field_by_label(soup, "Full title of the trial")
        if not trial_title:
            # Fallback to "Title of the trial for lay people"
            trial_title = extract_field_by_label(soup, "Title of the trial for lay people")

        # 4. Start Date / Date of Decision
        start_date = self._parse_trial_date(soup)

        # 5. Trial Status
        trial_status = extract_field_by_label(soup, "Trial Status")
        if not trial_status:
            trial_status = extract_field_by_label(soup, "Status of the trial")

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
        target_labels = [
            "Trade name",
            "Name of Active Substance",
            "Product Name",
            "Pharmaceutical form",
        ]

        pattern = re.compile(
            r"(" + "|".join([re.escape(label) for label in target_labels]) + r")", re.IGNORECASE
        )

        markers = soup.find_all(string=pattern)
        candidate_tables = []
        seen_ids = set()

        for marker in markers:
            parent_table = marker.find_parent("table")
            if parent_table and id(parent_table) not in seen_ids:
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
        drug_name = extract_field_by_label(soup, "Trade name")
        if not drug_name:
            drug_name = extract_field_by_label(soup, "Product Name")

        active_ingredient = extract_field_by_label(soup, "Name of Active Substance")
        if not active_ingredient:
            active_ingredient = extract_field_by_label(soup, "Active Substance")

        pharm_form = extract_field_by_label(soup, "Pharmaceutical form")
        cas_number = extract_field_by_label(soup, "CAS Number")

        if not any([drug_name, active_ingredient, pharm_form, cas_number]):
            return None

        return EuTrialDrug(
            eudract_number=eudract_number,
            drug_name=drug_name,
            active_ingredient=active_ingredient,
            cas_number=cas_number,
            pharmaceutical_form=pharm_form,
        )

    def _parse_trial_date(self, soup: BeautifulSoup) -> Optional[date]:
        """
        Attempt to parse the start date using priority logic.
        """
        date_str = extract_field_by_label(soup, "Date of Competent Authority Decision")
        if not date_str:
            date_str = extract_field_by_label(soup, "Date record first entered")

        return parse_flexible_date(date_str)
