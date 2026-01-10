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
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_euctr.main import run_bronze, run_silver
from coreason_etl_euctr.models import EuTrial


class MockTrial(EuTrial):
    pass


def test_run_silver_mixed_results(tmp_path: Path) -> None:
    """
    Test edge case where parser fails for some files but succeeds for others.
    """
    d = tmp_path / "bronze"
    d.mkdir()

    # Create 3 files
    (d / "success1.html").write_text("ok")
    (d / "fail.html").write_text("bad")
    (d / "success2.html").write_text("ok")

    mock_parser = MagicMock()
    # success1 -> OK, fail -> Error, success2 -> OK
    mock_parser.parse_trial.side_effect = [
        EuTrial(eudract_number="success1", url_source="s"),
        ValueError("Parse Error"),
        EuTrial(eudract_number="success2", url_source="s"),
    ]
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()

    run_silver(input_dir=str(d), parser=mock_parser, loader=mock_loader)

    # Ensure loader was called
    assert mock_loader.bulk_load_stream.called

    # Let's mock pipeline too to verify inputs
    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None
    mock_pipeline.stage_data.return_value = iter(["header", "row"])

    # Reset parser side_effect for the second run
    mock_parser.parse_trial.side_effect = [
        EuTrial(eudract_number="success1", url_source="s"),
        ValueError("Parse Error"),
        EuTrial(eudract_number="success2", url_source="s"),
    ]

    run_silver(input_dir=str(d), parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Verify pipeline.stage_data was called with a list of 2 trials
    args, _ = mock_pipeline.stage_data.call_args
    data_arg = args[0]
    assert len(data_arg) == 2
    assert data_arg[0].eudract_number == "success1"
    assert data_arg[1].eudract_number == "success2"


def test_run_silver_large_number_of_files(tmp_path: Path) -> None:
    """
    Test iteration over a larger number of files to ensure no resource exhaustion.
    """
    d = tmp_path / "bronze"
    d.mkdir()

    # Create 50 files
    for i in range(50):
        (d / f"{i}.html").write_text("content")

    mock_parser = MagicMock()
    mock_parser.parse_trial.side_effect = [EuTrial(eudract_number=str(i), url_source="s") for i in range(50)]
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None
    mock_pipeline.stage_data.return_value = iter(["header"])

    run_silver(input_dir=str(d), parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Verify all 50 were collected
    args, _ = mock_pipeline.stage_data.call_args
    assert len(args[0]) == 50


def test_run_bronze_mkdir_failure(tmp_path: Path) -> None:
    """Test defensive code when mkdir fails."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    output_dir = tmp_path / "bronze"
    # Ensure it exists before patching, so open() works later (if we let it)
    output_dir.mkdir(parents=True, exist_ok=True)

    # We patch Path.mkdir to raise exception
    with patch("pathlib.Path.mkdir", side_effect=PermissionError("Boom")):
        # We also mock open because run_bronze calls open on ids.csv
        # and checking ids_file.parent.exists() might be tricky if we don't mock it.
        # But actually run_bronze does:
        # if not ids_file.parent.exists():
        #    ids_file.parent.mkdir(...)
        # So we need exists() to return False to trigger mkdir.

        with patch("pathlib.Path.exists", return_value=False):
            with patch("builtins.open", new_callable=MagicMock):
                run_bronze(output_dir=str(output_dir), crawler=mock_crawler, downloader=mock_downloader)

    # If we reached here without crash, success. Logic logs warning.


def test_run_bronze_read_ids_failure(tmp_path: Path) -> None:
    """Test failure when reading the intermediate IDs file."""
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()

    output_dir = tmp_path / "bronze"
    output_dir.mkdir()
    ids_file = output_dir / "ids.csv"
    ids_file.write_text("ID1")

    # We want write to succeed (real file) but read to fail.
    # We can patch open to fail only when mode='r'.

    real_open = open

    def side_effect(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if "r" in mode and str(file) == str(ids_file):
            raise OSError("Read Error")
        return real_open(file, mode, *args, **kwargs)

    with patch("builtins.open", side_effect=side_effect):
        run_bronze(output_dir=str(output_dir), crawler=mock_crawler, downloader=mock_downloader)

    # Should log error and return (skipping download)
    assert mock_downloader.download_trial.call_count == 0


def test_run_silver_storage_read_error(tmp_path: Path) -> None:
    """Test resilience when storage read fails for a specific file."""
    # Setup dummy files
    backend = MagicMock()
    # Mock listing to return 2 files
    from coreason_etl_euctr.storage import StorageObject

    backend.list_files.return_value = iter(
        [StorageObject(key="good.html", mtime=100), StorageObject(key="bad.html", mtime=100)]
    )

    # Mock read to fail for one
    def read_side_effect(key: str) -> str:
        if key == "bad.html":
            raise OSError("Read failed")
        return "<html>content</html>"

    backend.read.side_effect = read_side_effect

    mock_parser = MagicMock()
    mock_parser.parse_trial.return_value = EuTrial(eudract_number="123", url_source="s")
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()

    # We must patch Pipeline watermark to ensure files are processed
    # Or just rely on default pipeline mock in run_silver if passed?
    # run_silver instantiates Pipeline() if None.
    # We should inject it.
    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None
    mock_pipeline.stage_data.return_value = iter(["header"])

    run_silver(
        input_dir="dummy", storage_backend=backend, parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader
    )

    # Should have processed 'good.html' (parsed 1 trial)
    # 'bad.html' should have been logged and skipped

    # Verify stage_data called with 1 item
    args, _ = mock_pipeline.stage_data.call_args
    assert len(args[0]) == 1
    assert args[0][0].eudract_number == "123"


def test_run_silver_storage_list_error() -> None:
    """Test immediate failure if list_files raises exception."""
    backend = MagicMock()
    backend.list_files.side_effect = Exception("S3 Down")

    # This exception should bubble up or be handled?
    # Current implementation: list(storage.list_files(...)) is called directly.
    # It is NOT wrapped in try-except in run_silver.
    # So it should raise.

    with pytest.raises(Exception, match="S3 Down"):
        run_silver(input_dir="dummy", storage_backend=backend)
