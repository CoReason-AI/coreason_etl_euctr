# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import argparse
import os
import sys
from typing import Optional, Sequence

from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"))


class ETLConfig:
    """Configuration for the ETL pipeline."""

    def __init__(self, full_load: bool = False):
        self.full_load = full_load
        # Defaults per R.6.2.1
        self.target_countries = ["3rd", "GB", "DE"]
        self.sleep_seconds = 1.0


def run_pipeline(config: ETLConfig) -> None:
    """
    Orchestrates the ETL pipeline.
    """
    mode = "FULL" if config.full_load else "DELTA"
    logger.info(f"Starting ETL Pipeline in {mode} mode.")

    if config.full_load:
        logger.info("Triggering Full Re-crawl...")
    else:
        logger.info("Triggering Delta Load...")

    logger.info("Pipeline finished.")


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EU CTR ETL Pipeline")

    parser.add_argument(
        "--full",
        action="store_true",
        help="Perform a full re-crawl of all search pages (ignoring High-Water Mark).",
    )

    return parser.parse_args(args)


def main() -> None:  # pragma: no cover
    args = parse_args()
    config = ETLConfig(full_load=args.full)
    run_pipeline(config)


if __name__ == "__main__":  # pragma: no cover
    main()
