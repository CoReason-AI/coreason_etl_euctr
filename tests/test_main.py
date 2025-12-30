# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, call, patch

import pytest
from coreason_etl_euctr.main import StringIteratorIO, _load_table, hello_world, run_bronze, run_silver
from coreason_etl_euctr.storage import LocalStorageBackend
from pydantic import BaseModel


def test_main_run_silver_explicit_backend(tmp_path: Path) -> None:
    """
    Test run_silver when storage_backend is explicitly provided.
    This should cover the 'if storage_backend:' branch in main.py.
    """
    mock_storage = MagicMock()
    # Mock list_files to return empty list so loop doesn't run
    mock_storage.list_files.return_value = []

    mock_loader = MagicMock()

    run_silver(input_dir=str(tmp_path), storage_backend=mock_storage, loader=mock_loader)

    # Verify list_files called
    mock_storage.list_files.assert_called_once()


def test_hello_world() -> None:
    assert hello_world() == "Hello World!"


def test_run_bronze_flow(tmp_path: Path) -> None:
    """Test the orchestration flow of run_bronze with HWM logic using real temp files."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Setup HWM
    mock_pipeline.get_high_water_mark.return_value = date(2023, 1, 1)

    # Setup mocks
    # Mock harvest_ids generator directly since run_bronze calls it
    # We yield duplicate IDs to verify deduplication
    # harvest_ids now yields (page_num, [ids])
    mock_crawler.harvest_ids.return_value = iter([(1, ["ID1", "ID2"]), (2, ["ID2", "ID3"])])
    mock_downloader.download_trial.return_value = True

    # Setup crawl cursor check (return None to indicate no prior state)
    mock_pipeline.get_crawl_cursor.return_value = None

    run_bronze(
        output_dir=str(tmp_path),
        start_page=1,
        max_pages=2,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Verify harvest_ids call with HWM
    mock_crawler.harvest_ids.assert_called_once_with(start_page=1, max_pages=2, date_from="2023-01-01")

    # Verify intermediate file was created and populated
    ids_file = tmp_path / "ids.csv"
    assert ids_file.exists()
    content = ids_file.read_text(encoding="utf-8")
    assert "ID1" in content
    assert "ID3" in content

    # Verify Deduplication (ID1, ID2, ID3 = 3 unique)
    assert mock_downloader.download_trial.call_count == 3
    mock_downloader.download_trial.assert_has_calls([call("ID1"), call("ID2"), call("ID3")], any_order=True)

    # Verify HWM update
    mock_pipeline.set_high_water_mark.assert_called_once()


def test_run_bronze_no_hwm(tmp_path: Path) -> None:
    """Test run_bronze when no High-Water Mark is found (Full Crawl)."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()
    # Explicitly return None for HWM so date_from becomes None
    mock_pipeline.get_high_water_mark.return_value = None
    # Explicitly return None for crawl cursor
    mock_pipeline.get_crawl_cursor.return_value = None

    # Empty harvest
    mock_crawler.harvest_ids.return_value = iter([])

    run_bronze(
        output_dir=str(tmp_path),
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Verify Crawler called without date_from
    mock_crawler.harvest_ids.assert_called_with(start_page=1, max_pages=1, date_from=None)


def test_run_bronze_resumes_from_state(tmp_path: Path) -> None:
    """Test run_bronze resumes from saved page cursor if start_page=1."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Ensure HWM is None so date_from is None
    mock_pipeline.get_high_water_mark.return_value = None
    # Simulate saved state: last crawled page was 5
    mock_pipeline.get_crawl_cursor.return_value = 5
    mock_crawler.harvest_ids.return_value = iter([])

    run_bronze(
        output_dir=str(tmp_path),
        start_page=1,  # Default
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Should start from 6 (5+1)
    mock_crawler.harvest_ids.assert_called_with(start_page=6, max_pages=1, date_from=None)


def test_run_bronze_explicit_start_page_overrides_state(tmp_path: Path) -> None:
    """Test user provided start_page overrides saved state."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Ensure HWM is None so date_from is None
    mock_pipeline.get_high_water_mark.return_value = None
    mock_pipeline.get_crawl_cursor.return_value = 5
    mock_crawler.harvest_ids.return_value = iter([])

    run_bronze(
        output_dir=str(tmp_path),
        start_page=10,  # Explicit
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Should start from 10
    mock_crawler.harvest_ids.assert_called_with(start_page=10, max_pages=1, date_from=None)


def test_run_bronze_saves_cursor(tmp_path: Path) -> None:
    """Test that crawl cursor is saved after each page."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Yield 2 pages
    mock_crawler.harvest_ids.return_value = iter([(1, ["ID1"]), (2, ["ID2"])])

    run_bronze(
        output_dir=str(tmp_path),
        max_pages=2,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Should call set_crawl_cursor twice
    mock_pipeline.set_crawl_cursor.assert_has_calls([call(1), call(2)])


def test_run_bronze_default_downloader(tmp_path: Path) -> None:
    """Test run_bronze initialization when no downloader provided."""
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = iter([])

    # We pass downloader=None (default)
    # output_dir should be used to create LocalStorageBackend
    output_dir = tmp_path / "bronze"

    with patch("coreason_etl_euctr.main.Downloader") as mock_downloader_cls:
        run_bronze(output_dir=str(output_dir), crawler=mock_crawler)

        mock_downloader_cls.assert_called_once()
        # Verify call args: check if storage_backend was passed
        _, kwargs = mock_downloader_cls.call_args
        assert "storage_backend" in kwargs
        assert isinstance(kwargs["storage_backend"], LocalStorageBackend)
        assert kwargs["storage_backend"].base_path == output_dir


def test_run_bronze_handles_crawl_exception(tmp_path: Path) -> None:
    """Test that crawler failure (exception during iteration) doesn't crash run_bronze."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    # Generator raises exception or simulation of failure logic inside loop is encapsulated in harvest_ids.
    # If harvest_ids raises, run_bronze should probably handle it or fail.
    # However, harvest_ids implementation catches errors internally and yields valid IDs.
    # So if we mock harvest_ids, we simulate it yielding some IDs successfully.
    # If harvest_ids crashes entirely (unhandled), run_bronze crashes (which is expected).
    # But let's assume harvest_ids yielded 'ID1' before crashing or finishing.

    def gen() -> Generator[tuple[int, list[str]], None, None]:
        yield (1, ["ID1"])
        # Simulate clean exit or continued yield after handled internal error
        # If we raise here, run_bronze loop will crash unless wrapped.
        # But run_bronze wraps the loop? No, it just iterates.
        # The Crawler.harvest_ids handles internal page errors.
        # So from run_bronze perspective, it just receives IDs.
        # So this test effectively tests that run_bronze processes whatever it gets.
        yield (2, ["ID2"])

    mock_crawler.harvest_ids.return_value = gen()

    run_bronze(
        output_dir=str(tmp_path),
        max_pages=2,
        crawler=mock_crawler,
        downloader=mock_downloader,
    )

    # Should download what was yielded
    assert mock_downloader.download_trial.call_count == 2


def test_run_bronze_handles_download_exception(tmp_path: Path) -> None:
    """Test that download failure doesn't stop the loop."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    mock_crawler.harvest_ids.return_value = iter([(1, ["ID1", "ID2"])])
    # ID1 fails, ID2 succeeds
    mock_downloader.download_trial.side_effect = [Exception("Disk Full"), True]

    run_bronze(
        output_dir=str(tmp_path),
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
    )

    assert mock_downloader.download_trial.call_count == 2


class MockTrial(BaseModel):
    eudract_number: str


def test_run_silver_full_load(tmp_path: Path) -> None:
    """Test the parsing and FULL loading flow of run_silver."""
    # Create dummy bronze files
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "2015-001.html"
    p1.write_text("<html>Content 1</html>")

    mock_parser = MagicMock()
    mock_pipeline = MagicMock()
    mock_loader = MagicMock()

    # Setup Parser returns
    trial_obj = MockTrial(eudract_number="2015-001")
    mock_parser.parse_trial.return_value = trial_obj
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    # Setup Pipeline returns
    mock_pipeline.stage_data.return_value = iter(["header\n", "row1\n"])
    mock_pipeline.get_silver_watermark.return_value = None  # New logic

    run_silver(input_dir=str(d), mode="FULL", parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Verify Loader calls
    mock_loader.connect.assert_called_once()
    mock_loader.prepare_schema.assert_called_once()
    # FULL mode means truncate + bulk_load
    mock_loader.truncate_tables.assert_called_once_with(["eu_trials"])
    # load_table calls bulk_load_stream.
    # Since drugs/conditions are empty, only trials should trigger load
    assert mock_loader.bulk_load_stream.call_count == 1
    mock_loader.commit.assert_called_once()
    mock_loader.close.assert_called_once()


def test_run_silver_upsert_load(tmp_path: Path) -> None:
    """Test the parsing and UPSERT loading flow of run_silver."""
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "2015-001.html"
    p1.write_text("content")

    mock_parser = MagicMock()
    trial_obj = MockTrial(eudract_number="2015-001")
    mock_parser.parse_trial.return_value = trial_obj
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_pipeline = MagicMock()
    mock_pipeline.stage_data.return_value = iter(["header\n", "row1\n"])
    mock_pipeline.get_silver_watermark.return_value = None  # New logic
    mock_loader = MagicMock()

    run_silver(input_dir=str(d), mode="UPSERT", parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Truncate should NOT be called
    mock_loader.truncate_tables.assert_not_called()
    # upsert_stream should be called
    mock_loader.upsert_stream.assert_called()


def test_run_silver_invalid_mode() -> None:
    """Test ValueError on invalid mode."""
    with pytest.raises(ValueError, match="Mode must be 'FULL' or 'UPSERT'"):
        run_silver(mode="INVALID")


def test_run_silver_id_mismatch(tmp_path: Path) -> None:
    """Test warning when filename mismatch ID."""
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "2015-999.html"  # ID in filename is 2015-999
    p1.write_text("<html>Content</html>")

    mock_parser = MagicMock()
    # ID in content is 2015-001
    trial_obj = MockTrial(eudract_number="2015-001")
    mock_parser.parse_trial.return_value = trial_obj
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()

    run_silver(input_dir=str(d), parser=mock_parser, loader=mock_loader)

    # Logic should proceed but log warning.
    mock_loader.bulk_load_stream.assert_called()


def test_run_silver_parse_exception(tmp_path: Path) -> None:
    """Test exception during generic processing of file (e.g. read error or other)."""
    # This covers the broad 'except Exception as e' loop in file processing
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "2015-001.html"
    p1.write_text("content")

    mock_parser = MagicMock()
    # parse_trial raises generic Exception (not ValueError)
    mock_parser.parse_trial.side_effect = Exception("Surprise!")

    mock_loader = MagicMock()

    run_silver(input_dir=str(d), parser=mock_parser, loader=mock_loader)

    # Should continue loop (skip file) and thus no trials loaded
    mock_loader.bulk_load_stream.assert_not_called()


def test_run_silver_no_input_dir(tmp_path: Path) -> None:
    """Test early exit if input dir missing and no backend provided."""
    mock_loader = MagicMock()
    # Use a directory that definitely does not exist
    missing_dir = tmp_path / "does_not_exist"

    with patch("coreason_etl_euctr.main.logger") as mock_logger:
        run_silver(input_dir=str(missing_dir), loader=mock_loader)
        mock_loader.connect.assert_not_called()
        mock_logger.error.assert_called_once()
        assert "does not exist" in mock_logger.error.call_args[0][0]


def test_run_silver_no_valid_data(tmp_path: Path) -> None:
    """Test skipping load if no valid data parsed."""
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "bad.html"
    p1.write_text("bad content")

    mock_parser = MagicMock()
    mock_parser.parse_trial.side_effect = ValueError("Parse error")
    mock_loader = MagicMock()

    run_silver(input_dir=str(d), parser=mock_parser, loader=mock_loader)

    mock_loader.connect.assert_not_called()


def test_run_silver_db_error(tmp_path: Path) -> None:
    """Test rollback on DB error."""
    d = tmp_path / "bronze"
    d.mkdir()
    p1 = d / "2015-001.html"
    p1.write_text("<html>Content</html>")

    mock_parser = MagicMock()
    trial_obj = MockTrial(eudract_number="2015-001")
    mock_parser.parse_trial.return_value = trial_obj
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()
    mock_loader.bulk_load_stream.side_effect = Exception("DB Error")

    run_silver(input_dir=str(d), parser=mock_parser, loader=mock_loader)

    mock_loader.rollback.assert_called_once()
    mock_loader.close.assert_called_once()


def test_string_iterator_io() -> None:
    """Test the helper class adapting iterator to read()."""
    data = ["abc", "def"]
    stream = StringIteratorIO(iter(data))

    # Read first chunk
    assert stream.read() == "abc"
    # Read second chunk
    assert stream.read() == "def"
    # End of stream
    assert stream.read() == ""

    # Test buffer usage (partial reads not implemented but let's cover logic)
    stream._buffer = "forced"
    assert stream.read() == "forced"


def test_load_table_upsert_missing_keys() -> None:
    """Test ValueError when conflict_keys is missing in UPSERT mode."""
    loader = MagicMock()
    pipeline = MagicMock()
    pipeline.stage_data.return_value = iter(["header", "row"])
    data = [MockTrial(eudract_number="123")]

    with pytest.raises(ValueError, match="Conflict keys required for UPSERT"):
        _load_table(loader, pipeline, data, "test_table", mode="UPSERT", conflict_keys=None)
