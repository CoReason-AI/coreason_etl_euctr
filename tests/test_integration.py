# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from coreason_etl_euctr.main import run_bronze, run_silver
from coreason_etl_euctr.storage import StorageObject


@pytest.fixture  # type: ignore[misc]
def mock_crawler() -> MagicMock:
    return MagicMock()


@pytest.fixture  # type: ignore[misc]
def mock_downloader() -> MagicMock:
    return MagicMock()


@pytest.fixture  # type: ignore[misc]
def mock_pipeline() -> MagicMock:
    p = MagicMock()
    p.storage_backend = None # Attribute
    p.get_high_water_mark.return_value = None
    p.get_crawl_cursor.return_value = None
    p.get_silver_watermark.return_value = None
    # Default identify_new_files to return something so run_silver proceeds
    p.identify_new_files.return_value = [StorageObject(key="123.html", mtime=100.0)]
    p.stage_data.side_effect = lambda x: iter(["header\n", "row1\n", "row2\n"])
    return p


@pytest.fixture  # type: ignore[misc]
def mock_storage() -> MagicMock:
    return MagicMock()


@pytest.fixture  # type: ignore[misc]
def mock_loader() -> MagicMock:
    return MagicMock()


@pytest.fixture  # type: ignore[misc]
def mock_parser() -> MagicMock:
    p = MagicMock()
    # Mock parse_trial return
    trial = MagicMock()
    trial.eudract_number = "123"
    p.parse_trial.return_value = trial
    p.parse_drugs.return_value = []
    p.parse_conditions.return_value = []
    return p


def test_run_bronze_full_crawl(
    mock_crawler: MagicMock,
    mock_downloader: MagicMock,
    mock_pipeline: MagicMock,
    tmp_path: Path,
) -> None:
    """Test full crawl execution."""
    # Setup
    mock_crawler.harvest_ids.return_value = [(1, ["id1", "id2"])]
    mock_downloader.download_trial.return_value = True # Ensure bool check works without extra calls

    # Execute
    run_bronze(
        output_dir=str(tmp_path),
        start_page=1,
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Verify
    mock_pipeline.get_high_water_mark.assert_called_once()
    mock_crawler.harvest_ids.assert_called_once()

    # Downloader called for unique IDs
    assert mock_downloader.download_trial.call_count == 2
    mock_downloader.download_trial.assert_has_calls([call("id1"), call("id2")])

    # State updated
    mock_pipeline.set_high_water_mark.assert_called_once()


def test_run_silver_full_load(
    mock_parser: MagicMock,
    mock_pipeline: MagicMock,
    mock_loader: MagicMock,
    mock_storage: MagicMock,
) -> None:
    """Test silver layer full load."""
    # Setup
    # Pipeline.identify_new_files is called, so it should return files
    # The default fixture does this, but let's be explicit if we want to test flow
    # And run_silver will rely on pipeline.identify_new_files
    # But run_silver injects storage into pipeline if missing.
    # We passed mock_pipeline, so it uses that.

    mock_storage.read.return_value = "<html>Content</html>"

    # Execute
    run_silver(
        input_dir="dummy",
        mode="FULL",
        parser=mock_parser,
        pipeline=mock_pipeline,
        loader=mock_loader,
        storage_backend=mock_storage,
    )

    # Verify
    # run_silver injects storage
    assert mock_pipeline.storage_backend == mock_storage

    # It calls identify_new_files
    mock_pipeline.identify_new_files.assert_called_once()

    # Then reads the file returned by pipeline
    mock_storage.read.assert_called_with("123.html")

    # Parsing
    mock_parser.parse_trial.assert_called_once()

    # Loader
    mock_loader.connect.assert_called_once()
    mock_loader.prepare_schema.assert_called_once()
    mock_loader.truncate_tables.assert_called_once()
    mock_loader.bulk_load_stream.assert_called()
    mock_loader.commit.assert_called_once()
    mock_loader.close.assert_called_once()


def test_run_silver_upsert(
    mock_parser: MagicMock,
    mock_pipeline: MagicMock,
    mock_loader: MagicMock,
    mock_storage: MagicMock,
) -> None:
    """Test silver layer upsert load."""
    mock_storage.read.return_value = "<html>Content</html>"

    run_silver(
        mode="UPSERT",
        parser=mock_parser,
        pipeline=mock_pipeline,
        loader=mock_loader,
        storage_backend=mock_storage,
    )

    # Verify upsert calls
    mock_loader.upsert_stream.assert_called()
    mock_loader.truncate_tables.assert_not_called()
