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
AGENT INSTRUCTION: This module defines the EpistemicBronzeLoaderTask to manage
the ingestion of raw HTML blobs into the Bronze layer using dlt.
"""

from typing import Any

import dlt

from coreason_etl_euctr.utils.logger import logger


class EpistemicBronzeLoaderTask:
    """
    Manages the ingestion of raw downloaded HTML blobs into the Bronze layer via dlt.
    """

    def __init__(
        self, pipeline_name: str = "euctr_bronze_pipeline", destination: str = "duckdb", dataset_name: str = "bronze"
    ) -> None:
        """
        Initializes the Bronze Loader task with dlt pipeline configuration.
        """
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset_name = dataset_name

    def load_html_blobs(self, eudract_id: str, downloaded_htmls: dict[str, str]) -> dlt.common.pipeline.LoadInfo:
        """
        Loads the downloaded HTML content into the Bronze layer.

        Args:
            eudract_id: The EudraCT Number associated with the protocols.
            downloaded_htmls: A dictionary mapping country codes to their raw HTML content.

        Returns:
            The dlt LoadInfo object containing the results of the load operation.
        """
        if not downloaded_htmls:
            logger.warning(f"No HTML content to load for {eudract_id}")
            # Instead of returning None, return a dummy LoadInfo or raise an exception
            # We'll just construct an empty pipeline run to keep type consistency if possible,
            # or return a specific dict that mimics LoadInfo if necessary, but returning early
            # from a pipeline run with empty list works too.

        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset_name,
        )

        data_to_load: list[dict[str, Any]] = []
        for country_code, html_content in downloaded_htmls.items():
            data_to_load.append(
                {
                    "eudract_id": eudract_id,
                    "country_code": country_code,
                    "raw_html": html_content,
                }
            )

        logger.info(f"Loading {len(data_to_load)} HTML blobs into Bronze layer for {eudract_id}")

        if not data_to_load:
            # run with empty list will just initialize pipeline
            return pipeline.run([])

        load_info = pipeline.run(
            data_to_load,
            table_name="raw_html_blobs",
            write_disposition="append",
        )

        logger.info(f"Successfully loaded Bronze layer for {eudract_id}")
        return load_info
