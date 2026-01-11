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
import concurrent.futures
import io
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

from pydantic import BaseModel

from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.downloader import Downloader
from coreason_etl_euctr.loader import BaseLoader
from coreason_etl_euctr.logger import logger
from coreason_etl_euctr.parser import Parser
from coreason_etl_euctr.pipeline import Pipeline
from coreason_etl_euctr.postgres_loader import PostgresLoader
from coreason_etl_euctr.redshift_loader import RedshiftLoader
from coreason_etl_euctr.storage import LocalStorageBackend, S3StorageBackend, StorageBackend
from coreason_etl_euctr.worker import process_file_content


def run_bronze(
    output_dir: str = "data/bronze",
    start_page: int = 1,
    max_pages: int = 1,
    crawler: Optional[Crawler] = None,
    downloader: Optional[Downloader] = None,
    pipeline: Optional[Pipeline] = None,
    storage_backend: Optional[StorageBackend] = None,
    ignore_hwm: bool = False,
    sleep_seconds: float = 1.0,
    country_priority: Optional[List[str]] = None,
) -> None:
    """
    Execute the Bronze Layer workflow: Crawl -> Deduplicate -> Download.
    Includes Delta Logic using High-Water Mark.

    Args:
        output_dir: Directory to save HTML files (used if storage_backend is None).
        start_page: Page number to start crawling.
        max_pages: Number of pages to crawl.
        crawler: Optional injected Crawler instance.
        downloader: Optional injected Downloader instance.
        pipeline: Optional injected Pipeline instance (for state management).
        storage_backend: Optional injected StorageBackend (Local or S3).
        ignore_hwm: If True, ignore the High-Water Mark and force a full crawl (R.3.3.1).
        sleep_seconds: Rate limit sleep time (seconds).
        country_priority: List of country codes to try in order.
    """
    crawler = crawler or Crawler(sleep_seconds=sleep_seconds)

    if not downloader:
        storage = storage_backend or LocalStorageBackend(Path(output_dir))
        downloader = Downloader(storage_backend=storage, sleep_seconds=sleep_seconds, country_priority=country_priority)

    pipeline = pipeline or Pipeline()

    # R.3.2.2: Retrieve High-Water Mark
    date_from = None
    if not ignore_hwm:
        high_water_mark = pipeline.get_high_water_mark()
        date_from = high_water_mark.isoformat() if high_water_mark else None

    if date_from:
        logger.info(f"Performing Delta Crawl from {date_from}")
    elif ignore_hwm:
        logger.info("Performing Full Re-crawl (Ignoring HWM as requested)")
    else:
        logger.info("Performing Full Crawl (No HWM found)")

    # Check if we should resume from state
    if not ignore_hwm:
        crawl_cursor = pipeline.get_crawl_cursor()
        if start_page == 1 and crawl_cursor:
            start_page = crawl_cursor + 1
            logger.info(f"Resuming crawl from page {start_page} (based on saved state).")

    # R.3.1.1: Store harvested IDs in an intermediate file
    ids_file = Path(output_dir) / "ids.csv"
    try:
        if not ids_file.parent.exists():
            ids_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.warning(f"Could not create directory for ids.csv: {e}")

    # Track Max Date for HWM Update
    max_date_seen: Optional[date] = None

    # Step 1: Crawl
    with open(ids_file, "a", encoding="utf-8") as f:
        # R.3.1.1 & R.3.2.1: Iterate using Crawler.harvest_ids
        for page_num, items in crawler.harvest_ids(start_page=start_page, max_pages=max_pages, date_from=date_from):
            # items is List[Tuple[str, Optional[date]]]
            for trial_id, trial_date in items:
                f.write(f"{trial_id}\n")

                # Update max date seen
                if trial_date:
                    if max_date_seen is None or trial_date > max_date_seen:
                        max_date_seen = trial_date

            # Save state after each successful page
            pipeline.set_crawl_cursor(page_num)

    # Step 2: Read and Deduplicate
    unique_ids: List[str] = []
    if ids_file.exists():
        try:
            with open(ids_file, "r", encoding="utf-8") as f:
                all_ids = [line.strip() for line in f if line.strip()]
            unique_ids = list(dict.fromkeys(all_ids))
        except Exception as e:
            logger.error(f"Failed to read IDs from {ids_file}: {e}")
            return

    logger.info(f"Found {len(unique_ids)} unique trials to download.")

    # Step 3: Download
    success_count = 0
    for trial_id in unique_ids:
        context_logger = logger.bind(trial_id=trial_id)
        try:
            if downloader.download_trial(trial_id):
                success_count += 1
        except Exception as e:
            context_logger.error(f"Failed to download {trial_id}: {e}")

    # R.3.2.2: Update High-Water Mark with STRICT logic
    if max_date_seen:
        logger.info(f"Updating High-Water Mark to {max_date_seen} (Max date found in search results).")
        pipeline.set_high_water_mark(max_date_seen)
    else:
        if len(unique_ids) > 0:
            logger.warning("No dates extracted from search results. High-Water Mark NOT updated.")
        else:
            pass

    logger.info(f"Bronze run complete. Downloaded {success_count}/{len(unique_ids)} trials.")


def run_silver(
    input_dir: str = "data/bronze",
    mode: str = "FULL",
    parser: Optional[Parser] = None,
    pipeline: Optional[Pipeline] = None,
    loader: Optional[BaseLoader] = None,
    storage_backend: Optional[StorageBackend] = None,
) -> None:
    """
    Execute the Silver Layer workflow: Parse -> Stage -> Load.

    Args:
        input_dir: Directory containing raw HTML files (used if storage_backend is None).
        mode: Loading mode, either "FULL" or "UPSERT".
        parser: Optional Parser instance.
        pipeline: Optional Pipeline instance.
        loader: Optional Loader instance.
        storage_backend: Optional injected StorageBackend (Local or S3).
    """
    parser = parser or Parser()
    pipeline = pipeline or Pipeline()
    loader = loader or PostgresLoader()
    if mode not in ["FULL", "UPSERT"]:
        raise ValueError("Mode must be 'FULL' or 'UPSERT'")

    storage: StorageBackend
    if storage_backend:
        storage = storage_backend
    else:
        if not Path(input_dir).exists():
            logger.error(f"Input directory {input_dir} does not exist.")
            return
        storage = LocalStorageBackend(Path(input_dir))

    # R.6.1.2: Parallel Parsing Config
    storage_config = storage.get_config()

    trials = []
    drugs = []
    conditions = []

    files = list(storage.list_files("*.html"))
    logger.info(f"Found {len(files)} HTML files to process.")

    # R.3.2.3: Incremental Processing (Skip unchanged files)
    silver_watermark = pipeline.get_silver_watermark()
    current_run_start_time = time.time()

    files_to_process = []
    skipped_old_count = 0
    skipped_future_count = 0

    for f in files:
        mtime = f.mtime

        # Check Lower Bound (Old files)
        if silver_watermark and mtime <= silver_watermark:
            skipped_old_count += 1
            continue

        # Check Upper Bound (Future/Current Run files)
        if mtime > current_run_start_time:
            skipped_future_count += 1
            continue

        files_to_process.append(f)

    if skipped_old_count > 0:
        logger.info(f"Skipping {skipped_old_count} unchanged files (mtime <= {silver_watermark}).")
    if skipped_future_count > 0:
        logger.info(f"Skipping {skipped_future_count} future files (mtime > {current_run_start_time}).")

    logger.info(f"Processing {len(files_to_process)} new/modified files.")

    # R.6.1.2: Parallel Parsing with I/O in worker
    chunk_size = 50
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i in range(0, len(files_to_process), chunk_size):
            chunk = files_to_process[i : i + chunk_size]
            future_to_key = {}

            # Submit tasks passing CONFIG and FILE_KEY only
            for file_obj in chunk:
                file_key = file_obj.key
                # Do NOT read content here. Pass key and config.
                future = executor.submit(process_file_content, file_key, storage_config)
                future_to_key[future] = file_key

            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    result = future.result()
                    if result:
                        trial, trial_drugs, trial_conds = result
                        trials.append(trial)
                        drugs.extend(trial_drugs)
                        conditions.extend(trial_conds)
                except Exception as e:
                    logger.error(f"Worker failed for {key}: {e}")

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


def _get_storage_backend(args: argparse.Namespace) -> Optional[StorageBackend]:
    """
    Resolve Storage Backend configuration from CLI args and Env Vars.
    Priority: CLI > Env > None (Default).
    """
    # Use getattr to safely access args that might not be present if subparser didn't add them
    # although we plan to add them to both.
    s3_bucket = getattr(args, "s3_bucket", None) or os.getenv("EUCTR_S3_BUCKET")
    s3_prefix = str(getattr(args, "s3_prefix", None) or os.getenv("EUCTR_S3_PREFIX", "") or "")
    s3_region = getattr(args, "s3_region", None) or os.getenv("EUCTR_S3_REGION")

    if s3_bucket:
        logger.info(f"Using S3 Storage Backend: s3://{s3_bucket}/{s3_prefix}")
        return S3StorageBackend(bucket_name=s3_bucket, prefix=s3_prefix, region_name=s3_region)

    return None


def _add_s3_args(parser: argparse.ArgumentParser) -> None:
    """Helper to add S3 configuration arguments to a parser."""
    parser.add_argument("--s3-bucket", help="S3 Bucket name for storage")
    parser.add_argument("--s3-prefix", help="S3 Prefix for storage")
    parser.add_argument("--s3-region", help="AWS Region for S3")


@logger.catch(onerror=lambda _: sys.exit(1))  # type: ignore[misc]
def main() -> int:
    """
    CLI Entry Point.
    """
    parser = argparse.ArgumentParser(description="Coreason ETL EU CTR Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bronze / Crawl
    parser_crawl = subparsers.add_parser("crawl", help="Run the Bronze layer (Crawler/Downloader)")
    parser_crawl.add_argument("--output-dir", default="data/bronze", help="Directory to save HTML files (Local Only)")
    parser_crawl.add_argument("--start-page", type=int, default=1, help="Page number to start crawling")
    parser_crawl.add_argument("--max-pages", type=int, default=1, help="Number of pages to crawl")
    parser_crawl.add_argument(
        "--ignore-hwm", action="store_true", help="Ignore High-Water Mark and force full re-crawl"
    )
    # Configurable options
    parser_crawl.add_argument("--sleep-seconds", type=float, default=1.0, help="Seconds to sleep between requests")
    parser_crawl.add_argument(
        "--country-priority", nargs="+", default=["3rd", "GB", "DE"], help="List of country codes to prioritize"
    )
    _add_s3_args(parser_crawl)

    # Silver / Load
    parser_load = subparsers.add_parser("load", help="Run the Silver layer (Parser/Loader)")
    parser_load.add_argument("--input-dir", default="data/bronze", help="Directory containing raw HTML files")
    parser_load.add_argument("--mode", choices=["FULL", "UPSERT"], default="FULL", help="Loading mode")
    parser_load.add_argument(
        "--target-db",
        choices=["postgres", "redshift"],
        default="postgres",
        help="Target database (default: postgres)",
    )
    parser_load.add_argument("--iam-role", help="IAM Role ARN for Redshift COPY command")
    _add_s3_args(parser_load)

    args = parser.parse_args()

    if args.command == "crawl":
        storage = _get_storage_backend(args)
        run_bronze(
            output_dir=args.output_dir,
            start_page=args.start_page,
            max_pages=args.max_pages,
            storage_backend=storage,
            ignore_hwm=args.ignore_hwm,
            sleep_seconds=args.sleep_seconds,
            country_priority=args.country_priority,
        )
    elif args.command == "load":
        # S3 Support for Load
        storage = _get_storage_backend(args)

        loader: BaseLoader
        if args.target_db == "redshift":
            # For Redshift, S3 configuration is mandatory for staging
            s3_bucket = args.s3_bucket or os.getenv("EUCTR_S3_BUCKET")
            if not s3_bucket:
                logger.error("Redshift loader requires --s3-bucket or EUCTR_S3_BUCKET env var.")
                return 1

            s3_prefix = str(args.s3_prefix or os.getenv("EUCTR_S3_PREFIX", "") or "")
            s3_region = args.s3_region or os.getenv("EUCTR_S3_REGION")

            loader = RedshiftLoader(
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                region=s3_region,
                iam_role=args.iam_role,
            )
        else:
            loader = PostgresLoader()

        run_silver(input_dir=args.input_dir, mode=args.mode, storage_backend=storage, loader=loader)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
