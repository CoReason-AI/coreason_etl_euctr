# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import time
from typing import List, Optional, cast

import httpx
from bs4 import BeautifulSoup
from loguru import logger


class Crawler:
    """
    Crawler module responsible for harvesting EudraCT Numbers from the EU CTR search results.
    """

    BASE_URL = "https://www.clinicaltrialsregister.eu/ctr-search/search"

    def __init__(self, client: Optional[httpx.Client] = None) -> None:
        """
        Initialize the Crawler.

        Args:
            client: Optional httpx.Client instance. If not provided, a default one will be created.
        """
        self.client = client or httpx.Client(
            headers={"User-Agent": "Coreason-ETL-Crawler/1.0"}, follow_redirects=True
        )

    def fetch_search_page(self, page_num: int = 1, query: str = "") -> str:
        """
        Fetch the HTML content of a search result page.

        Args:
            page_num: The page number to fetch (1-indexed).
            query: The search query string (defaults to empty for all trials).

        Returns:
            The raw HTML content of the page.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        params = {"query": query, "page": page_num}

        # Politeness: implementation of R.3.4.1
        time.sleep(1)

        try:
            logger.debug(f"Fetching search page {page_num}...")
            response = self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            return cast(str, response.text)
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch page {page_num}: {e}")
            raise

    def extract_ids(self, html_content: str) -> List[str]:
        """
        Parse the search result HTML to extract EudraCT Numbers.

        Args:
            html_content: The raw HTML content of the search page.

        Returns:
            A list of unique EudraCT Numbers found on the page.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        ids: List[str] = []

        # Find all elements that contain the label text "EudraCT Number:"
        labels = soup.find_all(string=lambda text: "EudraCT Number:" in text if text else False)

        for label in labels:
            parent = label.parent
            if not parent:
                continue

            # Case 1: Label and Value are in the same text node / element
            # Example: <span>EudraCT Number: 2004-000015-26</span>
            full_text = parent.get_text(strip=True)
            if "EudraCT Number:" in full_text and len(full_text) > len("EudraCT Number:"):
                # Extract value from the same string
                cleaned = full_text.replace("EudraCT Number:", "").strip()
                if cleaned:
                    # IDs are usually the first token if there's trailing text
                    ids.append(cleaned.split()[0])
                continue

            # Case 2: Label and Value are siblings
            # Example: <b>EudraCT Number:</b> 2004-001234-56<br/>

            # Check the next sibling node (could be a Tag or NavigableString)
            next_node = parent.next_sibling

            # Skip pure whitespace/newlines
            while next_node and (isinstance(next_node, str) and not next_node.strip()):
                next_node = next_node.next_sibling

            if next_node:
                val = next_node.get_text(strip=True) if hasattr(next_node, 'get_text') else str(next_node).strip()
                if val:
                    ids.append(val.split()[0])

        # Deduplicate while preserving order
        unique_ids = list(dict.fromkeys(ids))
        logger.info(f"Extracted {len(unique_ids)} IDs from page.")
        return unique_ids
