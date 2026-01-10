# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from coreason_etl_euctr.main import run_silver
from coreason_etl_euctr.models import EuTrial


def test_incremental_windowing(tmp_path: Path) -> None:
    """
    Test strict time windowing:
    - Files older than watermark: Skipped.
    - Files within window (watermark < mtime <= cutoff): Processed.
    - Files newer than cutoff (future relative to run start): Skipped.
    """
    d = tmp_path / "bronze"
    d.mkdir()

    # Base Time T0
    T0 = 1000.0

    # Create Files
    f_old = d / "old.html"
    f_old.write_text("old")
    os.utime(f_old, (T0, T0))  # mtime = T0

    f_new = d / "new.html"
    f_new.write_text("new")
    os.utime(f_new, (T0 + 10, T0 + 10))  # mtime = T0 + 10

    f_future = d / "future.html"
    f_future.write_text("future")
    os.utime(f_future, (T0 + 100, T0 + 100))  # mtime = T0 + 100

    # Mock components
    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = T0
    mock_pipeline.stage_data.side_effect = lambda x: iter([f"header\nrow_{id(x)}\n"])

    mock_loader = MagicMock()

    # We mock ProcessPoolExecutor and verify what it is called with or what results it returns.
    # Actually, the windowing logic happens BEFORE submission to executor.
    # So we can just inspect what files were read/submitted.
    # But `run_silver` does loop over chunks.

    with (
        patch("coreason_etl_euctr.main.concurrent.futures.ProcessPoolExecutor") as MockExecutor,
        patch("coreason_etl_euctr.main.concurrent.futures.as_completed") as mock_as_completed,
    ):
        executor_instance = MockExecutor.return_value
        executor_instance.__enter__.return_value = executor_instance

        # When submit is called, we can inspect args
        # We need to return valid future so logic proceeds
        mock_future = MagicMock()
        mock_future.result.return_value = (EuTrial(eudract_number="new", url_source="s"), [], [])
        executor_instance.submit.return_value = mock_future
        mock_as_completed.return_value = [mock_future]

        # Run with mocked time.time() = T0 + 50
        # Window is (1000, 1050].
        # Old (1000): Skipped (<= 1000).
        # New (1010): Processed.
        # Future (1100): Skipped (> 1050).

        with patch("time.time", return_value=T0 + 50):
            run_silver(input_dir=str(d), pipeline=mock_pipeline, loader=mock_loader)

        # Verify Logic via submit calls
        # submit(process_file_content, content, key, source)

        # We expect only "new.html" to be submitted.
        # old.html (mtime=T0) <= T0 (watermark) -> Skip
        # future.html (mtime=T0+100) > T0+50 (start time) -> Skip

        assert executor_instance.submit.call_count == 1
        call_args = executor_instance.submit.call_args
        # args[1] is file_key (if we follow order: func, content, key, source)
        # process_file_content signature: (content, file_key, url_source)
        # submit(func, *args)
        # args[0] is process_file_content
        # args[1] is content
        # args[2] is file_key

        file_key = call_args[0][2]
        assert file_key == "new.html"

    # Verify Watermark Update
    mock_pipeline.set_silver_watermark.assert_called_once_with(T0 + 50)


def test_incremental_rollback(tmp_path: Path) -> None:
    """
    Test that watermark is NOT updated if the load process fails.
    """
    d = tmp_path / "bronze"
    d.mkdir()
    f = d / "test.html"
    f.write_text("content")

    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None  # First run

    mock_loader = MagicMock()
    mock_loader.bulk_load_stream.side_effect = Exception("DB Error")

    with (
        patch("coreason_etl_euctr.main.concurrent.futures.ProcessPoolExecutor") as MockExecutor,
        patch("coreason_etl_euctr.main.concurrent.futures.as_completed") as mock_as_completed,
    ):
        executor_instance = MockExecutor.return_value
        executor_instance.__enter__.return_value = executor_instance

        mock_future = MagicMock()
        mock_future.result.return_value = (EuTrial(eudract_number="123", url_source="s"), [], [])
        executor_instance.submit.return_value = mock_future
        mock_as_completed.return_value = [mock_future]

        # Run
        run_silver(input_dir=str(d), pipeline=mock_pipeline, loader=mock_loader)

    # Verify Watermark NOT updated
    mock_pipeline.set_silver_watermark.assert_not_called()
    # Verify Rollback called
    mock_loader.rollback.assert_called_once()
