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
AGENT INSTRUCTION: This module defines the EpistemicPipelineOrchestratorTask
to coordinate the entire ETL pipeline from Harvester to Gold Aggregator.
It also provides the CLI entry point for the application.
"""

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from coreason_etl_euctr.utils.hashing import generate_deterministic_hash
from coreason_etl_euctr.utils.logger import logger
from coreason_etl_euctr.utils.state_manager import EpistemicStateManagerTask


class EpistemicPipelineOrchestratorTask:
    """
    Coordinates the execution of the EU CTR ETL pipeline.
    """

    def __init__(self) -> None:
        """
        Initializes the Orchestrator.
        """

    def run(self, auto_mode: bool = False, ids_file: str | None = None, full_mode: bool = False) -> None:
        """
        Executes the ETL pipeline based on the provided mode.

        Args:
            auto_mode: If True, uses the Harvester to discover new IDs.
            ids_file: If provided, reads IDs from the specified file path.
            full_mode: If True, bypasses download and reprocesses all existing Bronze HTML blobs.
        """
        ids: list[str] = []

        from coreason_etl_euctr.aggregator import EpistemicGoldAggregatorTask
        from coreason_etl_euctr.bronze_loader import EpistemicBronzeLoaderTask
        from coreason_etl_euctr.downloader import EpistemicDownloaderTask
        from coreason_etl_euctr.gold_loader import EpistemicGoldLoaderTask
        from coreason_etl_euctr.parser import EpistemicParserTask

        bronze_loader = EpistemicBronzeLoaderTask()
        parser = EpistemicParserTask()
        aggregator = EpistemicGoldAggregatorTask()
        gold_loader = EpistemicGoldLoaderTask(destination="postgres")

        state_manager = EpistemicStateManagerTask()

        silver_data = []

        if full_mode:
            logger.info("Starting pipeline in FULL mode. Reprocessing all existing Bronze data.")
            all_blobs = bronze_loader.read_all_html_blobs()

            if not all_blobs:
                logger.warning("No HTML blobs found in Bronze layer to reprocess.")
                return

            for eudract_id, downloaded_htmls in all_blobs.items():
                logger.info(f"Reprocessing {eudract_id}")
                for html_content in downloaded_htmls.values():
                    parsed_data = parser.parse_html(html_content)
                    parsed_data["A.2"] = eudract_id
                    silver_data.append(parsed_data)

            if silver_data:
                logger.info("Aggregating Silver data into Gold Polars DataFrame.")
                gold_df = aggregator.aggregate(silver_data)
                logger.info("Loading Gold DataFrame via dlt in REPLACE mode.")
                gold_loader.load_gold_dataframe(gold_df, write_disposition="replace")
            else:
                logger.warning("No Silver data generated from Bronze layer.")

            # Full mode completes here
            return

        # Regular incremental processing
        if auto_mode:
            logger.info("Starting pipeline in AUTO mode.")
            from coreason_etl_euctr.harvester import EpistemicHarvesterTask

            harvester = EpistemicHarvesterTask()
            date_from = state_manager.last_run_timestamp

            # Use only YYYY-MM-DD for dateFrom if a timestamp exists
            date_from_str = None
            if date_from:
                date_from_str = date_from.split("T")[0]
                logger.info(f"Using High-Water Mark: {date_from_str}")

            ids = harvester.harvest(date_from=date_from_str)
        elif ids_file:
            logger.info(f"Starting pipeline in IDS_FILE mode using: {ids_file}")
            try:
                with open(ids_file) as f:
                    # Read lines, strip whitespace, ignore empty lines and deduplicate while preserving order
                    seen = set()
                    for line in f:
                        line = line.strip()
                        if line and line not in seen:
                            seen.add(line)
                            ids.append(line)
            except FileNotFoundError:
                logger.error(f"File not found: {ids_file}")
                return
            except Exception as e:
                logger.error(f"Error reading file {ids_file}: {e}")
                return
        else:
            logger.warning("No execution mode specified. Exiting.")
            return

        logger.info(f"Discovered {len(ids)} EudraCT Numbers for processing.")

        downloader = EpistemicDownloaderTask()

        for eudract_id in ids:
            logger.info(f"Processing {eudract_id}")
            # Step B: Download HTML
            downloaded_htmls = downloader.download_protocol_html(eudract_id)

            if not downloaded_htmls:
                logger.warning(f"No HTML downloaded for {eudract_id}, skipping to next ID.")
                continue

            # Transformation (Parse) & Idempotency Check
            is_modified = False
            parsed_documents = []

            for country_code, html_content in downloaded_htmls.items():
                parsed_data = parser.parse_html(html_content)
                parsed_data["A.2"] = eudract_id  # Ensure source_id is always available
                parsed_documents.append(parsed_data)

                # Check idempotency per document
                current_hash = generate_deterministic_hash(parsed_data)

                # We track idempotency using a combined key for eudract_id + country_code
                state_key = f"{eudract_id}_{country_code}"
                previous_hash = state_manager.get_hash(state_key)

                if current_hash != previous_hash:
                    is_modified = True
                    state_manager.update_hash(state_key, current_hash)
                else:
                    logger.debug(f"Hash match for {eudract_id} in {country_code}, skipping downstream...")

            if not is_modified:
                logger.info(
                    f"No modifications detected for {eudract_id} across any geography. Skipping Bronze & Gold load."
                )
                continue

            # Bronze Layer Ingestion
            bronze_loader.load_html_blobs(eudract_id, downloaded_htmls)

            # Add to Silver data
            silver_data.extend(parsed_documents)

        if silver_data:
            # Gold Layer Aggregation
            logger.info("Aggregating Silver data into Gold Polars DataFrame.")
            gold_df = aggregator.aggregate(silver_data)

            # Gold Layer Loading
            logger.info("Loading Gold DataFrame via dlt in MERGE mode.")
            gold_loader.load_gold_dataframe(gold_df, write_disposition="merge")
        else:
            logger.warning("No Silver data collected, skipping Gold Layer Aggregation and Loading.")

        # Update last run timestamp upon successful completion
        if auto_mode:
            current_time = datetime.now(UTC).isoformat()
            state_manager.last_run_timestamp = current_time
            logger.info(f"Updated High-Water Mark to: {current_time}")


def parse_args(args: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Parses command-line arguments.

    Args:
        args: Optional list of command-line arguments to parse. Defaults to sys.argv[1:].

    Returns:
        An argparse.Namespace containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="EU CTR ETL Pipeline Orchestrator")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto", action="store_true", help="Run Harvester loop for new/updated trials.")
    group.add_argument(
        "--ids-file", type=str, metavar="PATH", help="Bypass Harvester and process specific IDs from file."
    )
    group.add_argument(
        "--full", action="store_true", help="Reprocess all existing Bronze HTML blobs into Silver/Gold layers."
    )

    return parser.parse_args(args)


def main(args: Sequence[str] | None = None) -> None:
    """
    CLI Entry point.

    Args:
        args: Optional list of command-line arguments.
    """
    parsed_args = parse_args(args)
    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(
        auto_mode=parsed_args.auto,
        ids_file=parsed_args.ids_file,
        full_mode=parsed_args.full,
    )


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
