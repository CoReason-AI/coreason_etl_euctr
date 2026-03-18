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

from coreason_etl_euctr.utils.logger import logger


class EpistemicPipelineOrchestratorTask:
    """
    Coordinates the execution of the EU CTR ETL pipeline.
    """

    def __init__(self) -> None:
        """
        Initializes the Orchestrator.
        """

    def run(self, auto_mode: bool = False, ids_file: str | None = None) -> None:
        """
        Executes the ETL pipeline based on the provided mode.

        Args:
            auto_mode: If True, uses the Harvester to discover new IDs.
            ids_file: If provided, reads IDs from the specified file path.
        """
        ids: list[str] = []

        if auto_mode:
            logger.info("Starting pipeline in AUTO mode.")
            from coreason_etl_euctr.harvester import EpistemicHarvesterTask

            harvester = EpistemicHarvesterTask()
            ids = harvester.harvest()
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

        from coreason_etl_euctr.aggregator import EpistemicGoldAggregatorTask
        from coreason_etl_euctr.bronze_loader import EpistemicBronzeLoaderTask
        from coreason_etl_euctr.downloader import EpistemicDownloaderTask
        from coreason_etl_euctr.gold_loader import EpistemicGoldLoaderTask
        from coreason_etl_euctr.parser import EpistemicParserTask

        downloader = EpistemicDownloaderTask()
        bronze_loader = EpistemicBronzeLoaderTask()
        parser = EpistemicParserTask()
        aggregator = EpistemicGoldAggregatorTask()
        gold_loader = EpistemicGoldLoaderTask()

        silver_data = []

        for eudract_id in ids:
            logger.info(f"Processing {eudract_id}")
            # Step B: Download HTML
            downloaded_htmls = downloader.download_protocol_html(eudract_id)

            if not downloaded_htmls:
                logger.warning(f"No HTML downloaded for {eudract_id}, skipping to next ID.")
                continue

            # Bronze Layer Ingestion
            bronze_loader.load_html_blobs(eudract_id, downloaded_htmls)

            # Transformation (Parse)
            for html_content in downloaded_htmls.values():
                parsed_data = parser.parse_html(html_content)
                parsed_data["A.2"] = eudract_id  # Ensure source_id is always available
                silver_data.append(parsed_data)

        if silver_data:
            # Gold Layer Aggregation
            logger.info("Aggregating Silver data into Gold Polars DataFrame.")
            gold_df = aggregator.aggregate(silver_data)

            # Gold Layer Loading
            logger.info("Loading Gold DataFrame via dlt.")
            gold_loader.load_gold_dataframe(gold_df, write_disposition="merge")
        else:
            logger.warning("No Silver data collected, skipping Gold Layer Aggregation and Loading.")


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

    return parser.parse_args(args)


def main(args: Sequence[str] | None = None) -> None:
    """
    CLI Entry point.

    Args:
        args: Optional list of command-line arguments.
    """
    parsed_args = parse_args(args)
    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(auto_mode=parsed_args.auto, ids_file=parsed_args.ids_file)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
