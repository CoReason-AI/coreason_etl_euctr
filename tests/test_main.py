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
from unittest.mock import MagicMock, call

import pytest
from coreason_etl_euctr.main import StringIteratorIO, _load_table, hello_world, run_bronze, run_silver
from pydantic import BaseModel


def test_hello_world() -> None:
    assert hello_world() == "Hello World!"


def test_run_bronze_flow() -> None:
    """Test the orchestration flow of run_bronze with HWM logic."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Setup HWM
    mock_pipeline.get_high_water_mark.return_value = date(2023, 1, 1)

    # Setup mocks
    mock_crawler.fetch_search_page.return_value = "<html>...</html>"
    mock_crawler.extract_ids.side_effect = [["ID1", "ID2"], ["ID2", "ID3"]]  # Simulate duplicates across pages
    mock_downloader.download_trial.return_value = True

    run_bronze(
        output_dir="tmp",
        start_page=1,
        max_pages=2,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Verify Crawler calls with date_from
    assert mock_crawler.fetch_search_page.call_count == 2
    mock_crawler.fetch_search_page.assert_has_calls(
        [call(page_num=1, date_from="2023-01-01"), call(page_num=2, date_from="2023-01-01")]
    )

    # Verify Deduplication (ID1, ID2, ID3 = 3 unique)
    assert mock_downloader.download_trial.call_count == 3
    mock_downloader.download_trial.assert_has_calls([call("ID1"), call("ID2"), call("ID3")], any_order=True)

    # Verify HWM update
    mock_pipeline.set_high_water_mark.assert_called_once()


def test_run_bronze_no_hwm() -> None:
    """Test run_bronze when no High-Water Mark is found (Full Crawl)."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.get_high_water_mark.return_value = None

    mock_crawler.fetch_search_page.return_value = "<html>...</html>"
    mock_crawler.extract_ids.return_value = []

    run_bronze(max_pages=1, crawler=mock_crawler, downloader=mock_downloader, pipeline=mock_pipeline)

    # Verify Crawler called without date_from
    mock_crawler.fetch_search_page.assert_called_with(page_num=1, date_from=None)


def test_run_bronze_handles_crawl_exception() -> None:
    """Test that crawler failure on one page doesn't stop the whole process."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    # Page 1 fails, Page 2 succeeds
    mock_crawler.fetch_search_page.side_effect = [Exception("Net Error"), "<html>OK</html>"]
    mock_crawler.extract_ids.return_value = ["ID1"]

    run_bronze(max_pages=2, crawler=mock_crawler, downloader=mock_downloader)

    # Should still try to download ID1
    mock_downloader.download_trial.assert_called_once_with("ID1")


def test_run_bronze_handles_download_exception() -> None:
    """Test that download failure doesn't stop the loop."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    mock_crawler.extract_ids.return_value = ["ID1", "ID2"]
    # ID1 fails, ID2 succeeds
    mock_downloader.download_trial.side_effect = [Exception("Disk Full"), True]

    run_bronze(max_pages=1, crawler=mock_crawler, downloader=mock_downloader)

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


def test_run_silver_no_input_dir() -> None:
    """Test early exit if input dir missing."""
    mock_loader = MagicMock()
    run_silver(input_dir="/non/existent", loader=mock_loader)
    mock_loader.connect.assert_not_called()


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
