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
import unicodedata
from typing import Generator, List, Optional

import httpx
from bs4 import BeautifulSoup, Comment

from coreason_etl_euctr.logger import logger
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from coreason_etl_euctr.utils import is_retryable_error


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
        self.client = client or httpx.Client(headers={"User-Agent": "Coreason-ETL-Crawler/1.0"}, follow_redirects=True)

    @retry(  # type: ignore[misc]
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_retryable_error),
        reraise=True,
    )
    def fetch_search_page(
        self, page_num: int = 1, query: str = "", date_from: Optional[str] = None, date_to: Optional[str] = None
    ) -> str:
        """
        Fetch the HTML content of a search result page.

        Args:
            page_num: The page number to fetch (1-indexed).
            query: The search query string (defaults to empty for all trials).
            date_from: Optional start date filter (YYYY-MM-DD).
            date_to: Optional end date filter (YYYY-MM-DD).

        Returns:
            The raw HTML content of the page.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        params = {"query": query, "page": str(page_num)}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        # Politeness: implementation of R.3.4.1
        time.sleep(1)

        try:
            logger.debug(f"Fetching search page {page_num}...")
            response = self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            return str(response.text)
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch page {page_num}: {e}")
            raise

    def harvest_ids(
        self,
        start_page: int = 1,
        max_pages: int = 1,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Generator[str, None, None]:
        """
        Iterate through search pages and yield EudraCT Numbers.
        Handles pagination and date filtering (CDC).

        Args:
            start_page: The starting page number.
            max_pages: Maximum number of pages to crawl.
            date_from: Start date for CDC (YYYY-MM-DD).
            date_to: End date for CDC (YYYY-MM-DD).

        Yields:
            EudraCT Numbers as strings.
        """
        end_page = start_page + max_pages
        for i in range(start_page, end_page):
            try:
                html = self.fetch_search_page(page_num=i, date_from=date_from, date_to=date_to)
                ids = self.extract_ids(html)

                if not ids:
                    logger.warning(f"No IDs found on page {i}. Stopping harvest.")
                    break

                for trial_id in ids:
                    yield trial_id

            except Exception as e:
                logger.error(f"Error harvesting page {i}: {e}")
                continue

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

        # Helper to normalize text (handles non-breaking spaces)
        def normalize(t: str) -> str:
            return unicodedata.normalize("NFKC", t)

        # Find all elements that contain the label text "EudraCT Number:"
        labels = soup.find_all(string=lambda text: "EudraCT Number:" in normalize(text) if text else False)

        for label in labels:
            # Ignore comments
            if isinstance(label, Comment):
                continue

            parent = label.parent
            if not parent:
                continue

            # Case 1: Label and Value are in the same text node / element
            # Example: <span>EudraCT Number: 2004-000015-26</span>
            full_text = normalize(parent.get_text(strip=True))
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
                raw_val = next_node.get_text(strip=True) if hasattr(next_node, "get_text") else str(next_node).strip()
                val = normalize(raw_val)
                if val:
                    ids.append(val.split()[0])

        # Deduplicate while preserving order
        unique_ids = list(dict.fromkeys(ids))
        logger.info(f"Extracted {len(unique_ids)} IDs from page.")
        return unique_ids
