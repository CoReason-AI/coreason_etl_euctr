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

from loguru import logger

from coreason_etl_euctr.pipeline import Pipeline
from coreason_etl_euctr.postgres_loader import PostgresLoader

logger.remove()
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"))


def run_bronze(args: argparse.Namespace) -> None:
    loader = PostgresLoader()  # Not used in bronze but pipeline requires it
    pipeline = Pipeline(loader, bronze_dir=args.output_dir)
    pipeline.run_bronze(query=args.query, start_page=args.start_page, max_pages=args.max_pages)


def run_silver(args: argparse.Namespace) -> None:
    loader = PostgresLoader()
    pipeline = Pipeline(loader, bronze_dir=args.input_dir)
    try:
        pipeline.run_silver(incremental=args.incremental)
    finally:
        loader.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="EU CTR ETL Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Bronze Command
    bronze = subparsers.add_parser("bronze", help="Run Bronze Layer (Crawl)")
    bronze.add_argument("--query", type=str, default="", help="Search query")
    bronze.add_argument("--start-page", type=int, default=1, help="Start page")
    bronze.add_argument("--max-pages", type=int, default=1, help="Max pages to crawl")
    bronze.add_argument("--output-dir", type=str, default="data/bronze", help="Output directory")
    bronze.set_defaults(func=run_bronze)

    # Silver Command
    silver = subparsers.add_parser("silver", help="Run Silver Layer (Parse & Load)")
    silver.add_argument("--input-dir", type=str, default="data/bronze", help="Input directory")
    silver.add_argument("--incremental", action="store_true", help="Use upsert logic")
    silver.set_defaults(func=run_silver)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()  # pragma: no cover
