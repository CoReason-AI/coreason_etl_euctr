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
AGENT INSTRUCTION: This module defines the EpistemicDownloaderTask to interact with the EU CTR portal
and download the raw HTML protocol pages for a given EudraCT Number across multiple countries.
"""

import time

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from coreason_etl_euctr.utils.logger import logger


class EpistemicDownloaderTask:
    """
    Manages the downloading of HTML protocol files from EU CTR for specific EudraCT Numbers.
    """

    BASE_URL = "https://www.clinicaltrialsregister.eu/ctr-search/trial"
    from coreason_etl_euctr.utils.config import settings

    TARGET_GEOGRAPHIES = settings.target_geographies

    def __init__(self, client: httpx.Client | None = None, rate_limit: float = 1.0) -> None:
        """
        Initializes the Downloader task with an HTTP client and rate limiting configuration.
        """
        self.client = client or httpx.Client()
        from coreason_etl_euctr.utils.config import settings

        self.rate_limit = rate_limit if rate_limit != 1.0 else settings.rate_limit

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_url_with_retry(self, url: str) -> httpx.Response:
        """
        Fetches a URL with exponential backoff for 5xx errors.
        """
        response = self.client.get(url)
        # Raise for 5xx errors to trigger retry
        if response.status_code >= 500:
            response.raise_for_status()
        return response

    def download_protocol_html(self, eudract_id: str) -> dict[str, str]:
        """
        Downloads the HTML protocols for a given EudraCT Number across all target geographies.

        Args:
            eudract_id: The EudraCT Number to download.

        Returns:
            A dictionary mapping country codes to their corresponding raw HTML content.
        """
        downloaded_htmls: dict[str, str] = {}

        for country_code in self.TARGET_GEOGRAPHIES:
            url = f"{self.BASE_URL}/{eudract_id}/{country_code}"
            logger.info(f"Downloading protocol for {eudract_id} ({country_code}) from {url}")

            try:
                response = self._fetch_url_with_retry(url)

                if response.status_code == 404:
                    logger.debug(f"Protocol not found for {eudract_id} in {country_code} (404)")
                else:
                    response.raise_for_status()
                    downloaded_htmls[country_code] = response.text
                    logger.info(f"Successfully downloaded protocol for {eudract_id} in {country_code}")

            except httpx.HTTPError as e:
                logger.error(f"Failed to download protocol for {eudract_id} in {country_code}: {e}")
            finally:
                # Politeness delay
                if self.rate_limit > 0:
                    time.sleep(self.rate_limit)

        return downloaded_htmls
