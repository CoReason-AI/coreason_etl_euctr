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
from datetime import date
from typing import Callable, Generator, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup, Comment, Tag
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from coreason_etl_euctr.logger import logger
from coreason_etl_euctr.utils import is_retryable_error, parse_flexible_date


class Crawler:
    """
    Crawler module responsible for harvesting EudraCT Numbers from the EU CTR search results.
    """

    BASE_URL = "https://www.clinicaltrialsregister.eu/ctr-search/search"

    def __init__(self, client: Optional[httpx.Client] = None, sleep_seconds: float = 1.0) -> None:
        """
        Initialize the Crawler.

        Args:
            client: Optional httpx.Client instance. If not provided, a default one will be created.
            sleep_seconds: Seconds to sleep between requests (rate limiting). Defaults to 1.0.
        """
        self.client = client or httpx.Client(headers={"User-Agent": "Coreason-ETL-Crawler/1.0"}, follow_redirects=True)
        self.sleep_seconds = sleep_seconds

    @retry(  # type: ignore[misc]
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_retryable_error),
        reraise=True,
    )
    def fetch_search_page(
        self,
        page_num: int = 1,
        query: str = "",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
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

        # Politeness: implementation of R.3.4.1 (Configurable)
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)

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
    ) -> Generator[Tuple[int, List[Tuple[str, Optional[date]]]], None, None]:
        """
        Iterate through search pages and yield (page_num, list_of_tuples).
        Each tuple is (EudraCT Number, Max Date Found for that Trial).

        Args:
            start_page: The starting page number.
            max_pages: Maximum number of pages to crawl.
            date_from: Start date for CDC (YYYY-MM-DD).
            date_to: End date for CDC (YYYY-MM-DD).

        Yields:
            Tuple containing the page number and a list of (id, date) found on that page.
        """
        end_page = start_page + max_pages
        for i in range(start_page, end_page):
            html = self.fetch_search_page(page_num=i, date_from=date_from, date_to=date_to)
            items = self.extract_ids(html)

            if not items:
                logger.warning(f"No IDs found on page {i}. Stopping harvest.")
                break

            yield (i, items)

    def extract_ids(self, html_content: str) -> List[Tuple[str, Optional[date]]]:
        """
        Parse the search result HTML to extract EudraCT Numbers AND Dates.
        Returns a list of tuples: (eudract_number, date_of_record).
        """
        soup = BeautifulSoup(html_content, "html.parser")
        items: List[Tuple[str, Optional[date]]] = []

        # The search result structure typically wraps each trial in a table or div.
        # We need to find the container to associate the ID with its date.
        # Assuming typical structure:
        # <table> ... <tr><td>EudraCT Number: ...</td> ... <td>Date ...: ...</td></tr> ... </table>
        # Or <div class="result"> ... </div>

        # Strategy: Find "EudraCT Number:" labels, then look for date within the same container.

        def normalize(t: str) -> str:
            return unicodedata.normalize("NFKC", t)

        def is_eudract_label(text: str) -> bool:
            return "EudraCT Number:" in normalize(text) if text else False

        labels = soup.find_all(string=is_eudract_label)

        for label in labels:
            if isinstance(label, Comment):
                continue

            parent = label.parent
            if not parent:
                continue

            # Find the ID
            trial_id = self._extract_id_from_label(parent, normalize)
            if not trial_id:
                continue

            # Find the Date within the closest common container (e.g., tr, table, div.result)
            container = parent.find_parent("table")
            if not container:
                container = parent.find_parent("div", class_="result")

            trial_date = None
            if container:
                trial_date = self._extract_date_from_container(container, normalize)

            items.append((trial_id, trial_date))

        # Deduplicate while preserving order. Use ID as key.
        seen = set()
        unique_items = []
        for item in items:
            if item[0] not in seen:
                seen.add(item[0])
                unique_items.append(item)

        logger.info(f"Extracted {len(unique_items)} IDs from page.")
        return unique_items

    def _extract_id_from_label(self, parent: Tag, normalize_func: Callable[[str], str]) -> Optional[str]:
        full_text = normalize_func(parent.get_text(strip=True))
        if "EudraCT Number:" in full_text and len(full_text) > len("EudraCT Number:"):
            cleaned = full_text.replace("EudraCT Number:", "").strip()
            if cleaned:
                return cleaned.split()[0]

        next_node = parent.next_sibling
        while next_node and (isinstance(next_node, str) and not next_node.strip()):
            next_node = next_node.next_sibling

        if next_node:
            raw_val = next_node.get_text(strip=True) if hasattr(next_node, "get_text") else str(next_node).strip()
            val = normalize_func(raw_val)
            if val:
                return val.split()[0]
        return None

    def _extract_date_from_container(self, container: Tag, normalize_func: Callable[[str], str]) -> Optional[date]:
        # Look for "Date of Competent Authority Decision" or "Date record first entered"
        # within the container.
        date_labels = ["Date of Competent Authority Decision", "Date record first entered"]

        for label_text in date_labels:
            target = container.find(string=lambda text: label_text in normalize_func(text) if text else False)  # noqa: B023
            if target:
                # Value is likely next sibling or parent's next sibling
                parent = target.parent
                if not parent:
                    continue

                # Try finding value in next sibling
                next_node = parent.next_sibling
                while next_node and (isinstance(next_node, str) and not next_node.strip()):
                    next_node = next_node.next_sibling

                val_text = ""
                if next_node:
                    val_text = (
                        next_node.get_text(strip=True) if hasattr(next_node, "get_text") else str(next_node).strip()
                    )

                # Sometimes it's in the same text node: "Date...: 2023-01-01"
                full_text = normalize_func(parent.get_text(strip=True))
                if ":" in full_text and len(full_text) > len(label_text):
                    # crude split
                    possible = full_text.split(":", 1)[1].strip()
                    if possible:
                        val_text = possible

                if val_text:
                    # Clean it
                    val_text = val_text.strip()
                    # Try parse
                    try:
                        # Attempt to take first date-like token if multiple words
                        # But dates can be "12 Jan 2023".
                        # Let's try parsing the whole string first
                        return parse_flexible_date(val_text)
                    except ValueError:
                        # Fallback: simple regex or split?
                        pass
        return None
