# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

"""
AGENT INSTRUCTION: This module defines the EpistemicHarvesterTask to interact with the EU CTR portal
and harvest EudraCT IDs via Advanced Search using pagination and date constraints.
"""

import time

import httpx
from bs4 import BeautifulSoup

from coreason_etl_euctr.utils.logger import logger


class EpistemicHarvesterTask:
    """
    Manages the harvesting of EudraCT Numbers from EU CTR search result pages.
    """

    BASE_URL = "https://www.clinicaltrialsregister.eu/ctr-search/search"

    def __init__(self, client: httpx.Client | None = None, rate_limit: float = 1.0) -> None:
        """
        Initializes the Harvester task with an HTTP client and rate limiting configuration.
        """
        self.client = client or httpx.Client()
        from coreason_etl_euctr.utils.config import settings

        self.rate_limit = rate_limit if rate_limit != 1.0 else settings.rate_limit

    def extract_ids_from_html(self, html_content: str) -> list[str]:
        """
        Parses the HTML search result page and extracts EudraCT Numbers.

        Args:
            html_content: The HTML content of the search result page.

        Returns:
            A list of unique EudraCT Numbers found on the page.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        ids = []
        for span in soup.find_all("span", class_="label"):
            if span.text and "EudraCT Number:" in span.text:
                # The ID is usually the next sibling node (Text node)
                next_sibling = span.next_sibling
                if next_sibling and isinstance(next_sibling, str):
                    eudract_id = next_sibling.strip()
                    if eudract_id:
                        ids.append(eudract_id)

        # Deduplicate deterministically preserving order
        unique_ids = []
        for e_id in ids:
            if e_id not in unique_ids:
                unique_ids.append(e_id)
        return unique_ids

    def harvest(self, date_from: str | None = None, max_pages: int = 100) -> list[str]:
        """
        Iterates through the search result pages and collects all EudraCT Numbers.

        Args:
            date_from: A date string (e.g., YYYY-MM-DD) to pass to `dateFrom` parameter for CDC.
            max_pages: The maximum number of pages to iterate over to prevent infinite loops.

        Returns:
            A deterministically sorted list of EudraCT Numbers.
        """
        all_ids = []
        page = 1

        while page <= max_pages:
            params = {"query": "", "page": str(page)}
            if date_from:
                # Advanced Search uses advanced query strings often, but simple `dateFrom` may work
                params["dateFrom"] = date_from

            logger.info(f"Fetching search results for page {page}")
            try:
                response = self.client.get(self.BASE_URL, params=params)
                response.raise_for_status()
            except httpx.HTTPError as e:
                logger.error(f"HTTP error while fetching search results: {e}")
                break

            extracted_ids = self.extract_ids_from_html(response.text)

            if not extracted_ids:
                logger.info(f"No more EudraCT Numbers found, ending pagination at page {page}.")
                break

            all_ids.extend(extracted_ids)
            page += 1

            # Politeness delay
            if page <= max_pages:
                time.sleep(self.rate_limit)

        unique_sorted_ids = sorted(set(all_ids))
        logger.info(f"Harvest completed, total IDs found: {len(unique_sorted_ids)}")
        return unique_sorted_ids
