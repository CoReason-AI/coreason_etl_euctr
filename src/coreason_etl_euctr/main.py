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
import io
import os
import sys
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

from loguru import logger
from pydantic import BaseModel

from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.downloader import Downloader
from coreason_etl_euctr.loader import BaseLoader
from coreason_etl_euctr.parser import Parser
from coreason_etl_euctr.pipeline import Pipeline
from coreason_etl_euctr.postgres_loader import PostgresLoader

logger.remove()
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"))


def run_bronze(
    output_dir: str = "data/bronze",
    start_page: int = 1,
    max_pages: int = 1,
    crawler: Optional[Crawler] = None,
    downloader: Optional[Downloader] = None,
    pipeline: Optional[Pipeline] = None,
) -> None:
    """
    Execute the Bronze Layer workflow: Crawl -> Deduplicate -> Download.
    Includes Delta Logic using High-Water Mark.

    Args:
        output_dir: Directory to save HTML files.
        start_page: Page number to start crawling.
        max_pages: Number of pages to crawl.
        crawler: Optional injected Crawler instance.
        downloader: Optional injected Downloader instance.
        pipeline: Optional injected Pipeline instance (for state management).
    """
    crawler = crawler or Crawler()
    downloader = downloader or Downloader(output_dir=Path(output_dir))
    pipeline = pipeline or Pipeline()

    # R.3.2.2: Retrieve High-Water Mark
    high_water_mark = pipeline.get_high_water_mark()
    date_from = high_water_mark.isoformat() if high_water_mark else None
    if date_from:
        logger.info(f"Performing Delta Crawl from {date_from}")
    else:
        logger.info("Performing Full Crawl (No HWM found)")

    all_ids: List[str] = []

    # Step 1: Crawl
    for i in range(start_page, start_page + max_pages):
        try:
            logger.info(f"Crawling page {i}...")
            # R.3.2.1: Pass HWM to search
            html = crawler.fetch_search_page(page_num=i, date_from=date_from)
            ids = crawler.extract_ids(html)
            all_ids.extend(ids)
        except Exception as e:
            logger.error(f"Failed to crawl page {i}: {e}")
            continue

    # Step 2: Deduplicate
    unique_ids = list(dict.fromkeys(all_ids))
    logger.info(f"Found {len(unique_ids)} unique trials to download.")

    # Step 3: Download
    success_count = 0
    for trial_id in unique_ids:
        try:
            if downloader.download_trial(trial_id):
                success_count += 1
        except Exception as e:
            logger.error(f"Failed to download {trial_id}: {e}")

    # R.3.2.2: Update High-Water Mark (to today)
    # Ideally, we should update this based on the latest date found in the data or run start time.
    # The requirement says "track the maximum timestamp... processed".
    # Since we filter by Date From, we can safely set HWM to "now" assuming we caught everything up to now.
    from datetime import date

    pipeline.set_high_water_mark(date.today())

    logger.info(f"Bronze run complete. Downloaded {success_count}/{len(unique_ids)} trials.")


def run_silver(
    input_dir: str = "data/bronze",
    mode: str = "FULL",
    parser: Optional[Parser] = None,
    pipeline: Optional[Pipeline] = None,
    loader: Optional[BaseLoader] = None,
) -> None:
    """
    Execute the Silver Layer workflow: Parse -> Stage -> Load.

    Args:
        input_dir: Directory containing raw HTML files.
        mode: Loading mode, either "FULL" or "UPSERT".
        parser: Optional Parser instance.
        pipeline: Optional Pipeline instance.
        loader: Optional Loader instance.
    """
    parser = parser or Parser()
    pipeline = pipeline or Pipeline()
    loader = loader or PostgresLoader()

    if mode not in ["FULL", "UPSERT"]:
        raise ValueError("Mode must be 'FULL' or 'UPSERT'")

    input_path = Path(input_dir)
    if not input_path.exists():
        logger.error(f"Input directory {input_dir} does not exist.")
        return

    # We need to process separate streams for each target table
    # Since Pipeline consumes an iterator, we need to generate separate iterators or lists.
    # For simplicity and to match the 'bulk_load_stream' interface, we will:
    # 1. Collect all valid parsed models into temporary lists (or use tee, but lists are safer for now if fit in mem).
    #    However, to be truly streaming, we should probably do 3 passes or use a generator that yields different types.
    #    But 'bulk_load_stream' takes one stream at a time.
    #    Let's parse once and split into buckets.

    # NOTE: In a massive scale scenario, we would write to temp files on disk.
    # For this iteration, we'll accumulate in memory as per current architectural scope.

    trials = []
    drugs = []
    conditions = []

    files = list(input_path.glob("*.html"))
    logger.info(f"Found {len(files)} HTML files to process.")

    # R.3.2.3: Incremental Processing (Skip unchanged files)
    silver_watermark = pipeline.get_silver_watermark()
    current_run_start_time = 0.0
    import time

    current_run_start_time = time.time()

    # If this is the first run, watermark is None, so we process everything.
    # If we have a watermark, we skip files older than it.
    files_to_process = []
    skipped_count = 0

    for f in files:
        mtime = f.stat().st_mtime
        if silver_watermark and mtime <= silver_watermark:
            skipped_count += 1
            continue
        files_to_process.append(f)

    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} unchanged files (mtime <= {silver_watermark}).")
    logger.info(f"Processing {len(files_to_process)} new/modified files.")

    for file_path in files_to_process:
        try:
            content = file_path.read_text(encoding="utf-8")
            # Extract ID from filename? Or parse it? Spec says filename is ID.
            trial_id = file_path.stem

            # Parse Trial
            # We assume the file contains the source URL or we reconstruct it.
            # R.4.2.3 says metadata preserved. If we injected meta tag, we could read it.
            # For now, we'll placeholder source.
            url_source = f"file://{file_path.name}"

            try:
                trial = parser.parse_trial(content, url_source=url_source)
                # Ensure ID matches filename just in case
                if trial.eudract_number != trial_id:
                    logger.warning(f"Filename {trial_id} mismatch with content {trial.eudract_number}")
                trials.append(trial)
            except ValueError as e:
                logger.warning(f"Failed to parse trial from {file_path}: {e}")
                continue

            # Parse Drugs
            trial_drugs = parser.parse_drugs(content, trial_id)
            drugs.extend(trial_drugs)

            # Parse Conditions
            trial_conds = parser.parse_conditions(content, trial_id)
            conditions.extend(trial_conds)

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            continue

    if not trials:
        logger.warning("No valid data parsed. Skipping load.")
        return

    # Load to DB
    try:
        loader.connect()
        loader.prepare_schema()

        if mode == "FULL":
            logger.info("Running FULL load (Truncate + Insert)...")
            # R.5.1.1: Truncate tables first (cascade handles children)
            loader.truncate_tables(["eu_trials"])
            _load_table(loader, pipeline, trials, "eu_trials", mode="FULL")
            _load_table(loader, pipeline, drugs, "eu_trial_drugs", mode="FULL")
            _load_table(loader, pipeline, conditions, "eu_trial_conditions", mode="FULL")

        else:
            logger.info("Running UPSERT load...")
            # R.5.1.2: Upsert logic
            _load_table(loader, pipeline, trials, "eu_trials", mode="UPSERT", conflict_keys=["eudract_number"])
            # For children, we also use upsert now that we have unique constraints
            _load_table(
                loader,
                pipeline,
                drugs,
                "eu_trial_drugs",
                mode="UPSERT",
                conflict_keys=["eudract_number", "drug_name", "pharmaceutical_form"],
            )
            _load_table(
                loader,
                pipeline,
                conditions,
                "eu_trial_conditions",
                mode="UPSERT",
                conflict_keys=["eudract_number", "condition_name"],
            )

        loader.commit()

        # Update watermark only on success
        pipeline.set_silver_watermark(current_run_start_time)

        logger.info(f"Silver run ({mode}) complete.")

    except Exception as e:
        logger.error(f"Database load failed: {e}")
        if loader:
            loader.rollback()
    finally:
        if loader:
            loader.close()


class StringIteratorIO(io.TextIOBase):
    """
    Helper to adapt a generator of strings into a file-like object for copy().
    """

    def __init__(self, iterator: Iterator[str]):
        self._iterator = iterator
        self._buffer = ""

    def read(self, size: int | None = -1) -> str:
        # If we have buffer, return it
        if self._buffer:
            ret = self._buffer
            self._buffer = ""
            return ret

        # Otherwise fetch next chunk
        try:
            return next(self._iterator)
        except StopIteration:
            return ""


def _load_table(
    loader: BaseLoader,
    pipeline: Pipeline,
    data: Sequence[BaseModel],
    table_name: str,
    mode: str = "FULL",
    conflict_keys: Optional[List[str]] = None,
) -> None:
    if not data:
        return

    gen = pipeline.stage_data(data)
    stream = StringIteratorIO(gen)
    # The BaseLoader interface expects IO[str], and StringIteratorIO is a TextIOBase which is IO[str].
    # However, mypy is strict. Explicitly typing stream helps.

    if mode == "FULL":
        loader.bulk_load_stream(stream, table_name)  # type: ignore[arg-type]
    elif mode == "UPSERT":
        if not conflict_keys:
            raise ValueError(f"Conflict keys required for UPSERT on {table_name}")
        loader.upsert_stream(stream, table_name, conflict_keys=conflict_keys)  # type: ignore[arg-type]


def hello_world() -> str:
    # Kept for backward compatibility with existing tests until removed
    logger.info("Hello World!")
    return "Hello World!"


def main() -> int:
    """
    CLI Entry Point.
    """
    parser = argparse.ArgumentParser(description="Coreason ETL EU CTR Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bronze / Crawl
    parser_crawl = subparsers.add_parser("crawl", help="Run the Bronze layer (Crawler/Downloader)")
    parser_crawl.add_argument("--output-dir", default="data/bronze", help="Directory to save HTML files")
    parser_crawl.add_argument("--start-page", type=int, default=1, help="Page number to start crawling")
    parser_crawl.add_argument("--max-pages", type=int, default=1, help="Number of pages to crawl")

    # Silver / Load
    parser_load = subparsers.add_parser("load", help="Run the Silver layer (Parser/Loader)")
    parser_load.add_argument("--input-dir", default="data/bronze", help="Directory containing raw HTML files")
    parser_load.add_argument("--mode", choices=["FULL", "UPSERT"], default="FULL", help="Loading mode")

    args = parser.parse_args()

    if args.command == "crawl":
        run_bronze(output_dir=args.output_dir, start_page=args.start_page, max_pages=args.max_pages)
    elif args.command == "load":
        run_silver(input_dir=args.input_dir, mode=args.mode)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
