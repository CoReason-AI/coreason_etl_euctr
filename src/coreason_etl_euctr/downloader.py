# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import hashlib
import time
from pathlib import Path
from typing import List, Optional

import httpx
from loguru import logger


class Downloader:
    """
    Downloader module responsible for fetching full HTML protocols for specific Trial IDs.
    Implements the country fallback logic (3rd -> GB -> DE) and saves raw HTML to the Bronze layer.
    """

    BASE_URL_TEMPLATE = "https://www.clinicaltrialsregister.eu/ctr-search/trial/{id}/{country}"
    COUNTRY_PRIORITY: List[str] = ["3rd", "GB", "DE"]

    def __init__(self, output_dir: Path, client: Optional[httpx.Client] = None) -> None:
        """
        Initialize the Downloader.

        Args:
            output_dir: The directory where raw HTML files will be saved.
            client: Optional httpx.Client instance.
        """
        self.output_dir = output_dir
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.client = client or httpx.Client(
            headers={"User-Agent": "Coreason-ETL-Downloader/1.0"}, follow_redirects=True, timeout=30.0
        )

    def download_trial(self, eudract_number: str) -> bool:
        """
        Download the protocol for a given EudraCT Number.
        Tries countries in priority order. Saves the first successful response to disk.

        Args:
            eudract_number: The unique EudraCT identifier.

        Returns:
            True if download was successful and saved, False otherwise.
        """
        for country in self.COUNTRY_PRIORITY:
            url = self.BASE_URL_TEMPLATE.format(id=eudract_number, country=country)

            # Politeness delay
            time.sleep(1)

            try:
                logger.debug(f"Attempting to fetch trial {eudract_number} from {country}...")
                response = self.client.get(url)

                # Check for 404 explicitly or other errors
                if response.status_code == 404:
                    logger.debug(f"Trial {eudract_number} not found in {country} (404).")
                    continue

                response.raise_for_status()

                # Some sites return soft 404s or empty pages, but for now we assume 200 OK with content is good.
                # The FRD says: "If 404/Empty, fallback..."
                if not response.text.strip():
                    logger.warning(f"Trial {eudract_number} in {country} returned empty body.")
                    continue

                self._save_to_disk(eudract_number, response.text, country)
                logger.info(f"Successfully downloaded trial {eudract_number} from {country}.")
                return True

            except httpx.HTTPError as e:
                logger.warning(f"Request failed for {eudract_number} in {country}: {e}")
                # We continue to the next country on error
                continue

        logger.error(f"Failed to download trial {eudract_number} from any source.")
        return False

    def _save_to_disk(self, eudract_number: str, content: str, source_country: str) -> None:
        """
        Save the HTML content to the output directory.
        Calculates and stores SHA-256 hash in metadata.
        Checks for existing hash to detect unchanged content.

        Args:
            eudract_number: The trial ID.
            content: The HTML content.
            source_country: The country code where it was found.
        """
        # Filename convention from FRD: 2015-001234-56.html
        file_path = self.output_dir / f"{eudract_number}.html"
        meta_path = self.output_dir / f"{eudract_number}.meta"

        # Calculate Hash (R.3.2.3)
        new_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        # Check existing hash if meta exists
        if meta_path.exists():
            try:
                existing_meta = meta_path.read_text(encoding="utf-8")
                # Simple parsing of key=value
                meta_dict = {}
                for line in existing_meta.splitlines():
                    if "=" in line:
                        k, v = line.split("=", 1)
                        meta_dict[k.strip()] = v.strip()

                if meta_dict.get("hash") == new_hash:
                    logger.info(f"Content for {eudract_number} is unchanged (Hash match).")
                    # Requirement R.3.2.3: Skip writing HTML to preserve idempotency/avoid IO.
                    # But we MUST update metadata to show we checked it (update downloaded_at).
                    self._write_metadata(meta_path, source_country, eudract_number, new_hash)
                    return
            except Exception:
                # Ignore read errors, just overwrite
                pass

        # Write HTML
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

            self._write_metadata(meta_path, source_country, eudract_number, new_hash)

        except IOError as e:
            logger.error(f"Failed to write file for {eudract_number}: {e}")
            raise

    def _write_metadata(self, meta_path: Path, source_country: str, eudract_number: str, file_hash: str) -> None:
        """Helper to write the .meta sidecar file."""
        url = self.BASE_URL_TEMPLATE.format(id=eudract_number, country=source_country)
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(
                f"source_country={source_country}\n"
                f"url={url}\n"
                f"downloaded_at={time.time()}\n"
                f"hash={file_hash}"
            )
