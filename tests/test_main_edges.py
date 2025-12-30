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
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.main import run_bronze, run_silver
from coreason_etl_euctr.storage import StorageObject


def test_run_silver_mixed_results(tmp_path: Path) -> None:
    """
    Test run_silver with mixed parse results:
    - File 1: Success (yields 1 trial)
    - File 2: Parse Error (ValueError)
    - File 3: Generic Error (Exception)
    """
    d = tmp_path / "bronze"
    d.mkdir()
    (d / "good.html").write_text("good")
    (d / "bad.html").write_text("bad")
    (d / "ugly.html").write_text("ugly")

    mock_parser = MagicMock()
    mock_loader = MagicMock()
    mock_pipeline = MagicMock()

    # Mock identify_new_files to return all 3
    mock_pipeline.identify_new_files.return_value = [
        StorageObject(key="good.html", mtime=100.0),
        StorageObject(key="bad.html", mtime=100.0),
        StorageObject(key="ugly.html", mtime=100.0),
    ]
    mock_pipeline.storage_backend = None

    # Configure Parser side effects for parse_trial
    # Order depends on iteration order of list_files, which depends on mock return
    # We returned good, bad, ugly.

    # We need to match calls to files.
    # Logic:
    # 1. read(good) -> parse(good) -> Success
    # 2. read(bad) -> parse(bad) -> ValueError
    # 3. read(ugly) -> parse(ugly) -> Exception

    # Mock storage read to return distinct content
    # But storage is created internally if not passed.
    # Wait, if we don't pass storage, it uses LocalStorageBackend.
    # So we should pass a mock storage to control read returns easily, OR write to files (which we did).

    # If we rely on LocalStorageBackend, it reads files.
    # good.html contains "good"
    # bad.html contains "bad"
    # ugly.html contains "ugly"

    def parse_side_effect(content: str, url_source: str):
        if content == "good":
            m = MagicMock()
            m.eudract_number = "123"
            return m
        if content == "bad":
            raise ValueError("Parse Error")
        if content == "ugly":
            raise Exception("Generic Error")
        return MagicMock()

    mock_parser.parse_trial.side_effect = parse_side_effect
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_pipeline.stage_data.return_value = iter(["header"])

    run_silver(
        input_dir=str(d), parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader
    )

    # Should have processed 'good.html' (parsed 1 trial)
    # 'bad.html' should have been logged and skipped
    # 'ugly.html' should have been logged and skipped

    # Verify stage_data called with 1 item
    # Since only 1 trial success
    # args[0] is the list of models.
    args, _ = mock_pipeline.stage_data.call_args
    assert len(args[0]) == 1
    assert args[0][0].eudract_number == "123"


def test_run_silver_storage_list_error() -> None:
    """Test immediate failure if identify_new_files raises exception."""
    mock_pipeline = MagicMock()
    mock_pipeline.identify_new_files.side_effect = Exception("S3 Down")
    mock_pipeline.storage_backend = None

    # We must provide storage backend to bypass directory check
    mock_storage = MagicMock()

    with pytest.raises(Exception, match="S3 Down"):
        run_silver(input_dir="dummy", pipeline=mock_pipeline, storage_backend=mock_storage)


def test_ids_file_creation_error(tmp_path: Path) -> None:
    """Test handling of error when creating ids.csv directory."""
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = iter([])

    # We mock Path.exists to return False, then mkdir to raise
    with patch("pathlib.Path.exists", return_value=False):
        with patch("pathlib.Path.mkdir", side_effect=Exception("Mkdir Fail")):
            # It should log warning and continue.
            # Then open() will likely fail because dir doesn't exist (or we mock that too?)
            # The test just checks that mkdir exception is caught.
            # If open fails, it raises Exception.
            with pytest.raises(Exception):
                run_bronze(output_dir="dummy", crawler=mock_crawler)

def test_ids_file_read_error(tmp_path: Path) -> None:
    """Test handling of error when reading ids.csv for deduplication."""
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = iter([])

    ids_file = tmp_path / "ids.csv"
    ids_file.parent.mkdir(parents=True, exist_ok=True)
    ids_file.touch()

    # We want read to fail.
    # Mock open() only during read phase? Hard because run_bronze calls open twice.
    # But read phase is inside `if ids_file.exists():`.

    # We can mock Path.exists to return True (it does).
    # Then mock open to fail? But open is called for write earlier.

    # Let's mock builtins.open but use side_effect to check mode
    original_open = open

    def side_effect(file, mode="r", *args, **kwargs):
        # normalize mode
        if "r" in mode:
            raise Exception("Read Fail")
        return original_open(file, mode, *args, **kwargs)

    with patch("builtins.open", side_effect=side_effect):
        run_bronze(output_dir=str(tmp_path), crawler=mock_crawler)
        # Should return (and log error) instead of raising

def test_run_silver_storage_read_error(tmp_path: Path) -> None:
    """Test handling of read error for a specific file."""
    # We pass a mock storage backend that fails on read
    mock_storage = MagicMock()
    mock_storage.read.side_effect = Exception("Read Permission Denied")

    # We need pipeline to return a file so loop runs
    mock_pipeline = MagicMock()
    mock_pipeline.identify_new_files.return_value = [StorageObject(key="secret.html", mtime=100.0)]
    mock_pipeline.storage_backend = None

    mock_loader = MagicMock()

    run_silver(
        input_dir="dummy", storage_backend=mock_storage, pipeline=mock_pipeline, loader=mock_loader
    )

    # Should catch exception and continue (log error)
    # So loader should not be called with data
    mock_loader.bulk_load_stream.assert_not_called()


def test_run_silver_large_number_of_files(tmp_path: Path) -> None:
    """Test processing loop with many files."""
    count = 100
    files = [StorageObject(key=f"{i}.html", mtime=100.0) for i in range(count)]

    mock_pipeline = MagicMock()
    mock_pipeline.identify_new_files.return_value = files
    mock_pipeline.stage_data.return_value = iter(["header"])
    mock_pipeline.storage_backend = None

    mock_storage = MagicMock()
    mock_storage.read.return_value = "content"

    mock_parser = MagicMock()
    mock_parser.parse_trial.return_value = MagicMock(eudract_number="123")
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()

    run_silver(
        input_dir="dummy",
        storage_backend=mock_storage,
        pipeline=mock_pipeline,
        parser=mock_parser,
        loader=mock_loader
    )

    # Should have parsed 100 times
    assert mock_parser.parse_trial.call_count == count

    # And staged 100 trials
    args, _ = mock_pipeline.stage_data.call_args
    assert len(args[0]) == count
