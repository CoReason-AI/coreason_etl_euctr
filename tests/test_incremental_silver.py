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
import time
from pathlib import Path
from unittest.mock import MagicMock

from coreason_etl_euctr.main import run_silver
from coreason_etl_euctr.pipeline import Pipeline


def test_incremental_silver_skips_unchanged_files(tmp_path: Path) -> None:
    """
    Verify that run_silver skips files that haven't changed since the last run.
    """
    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    state_file = tmp_path / "state.json"

    # Create a dummy HTML file
    file_path = bronze_dir / "2024-001.html"
    file_path.write_text("<html><body>Test</body></html>", encoding="utf-8")

    # Set mtime to the past (e.g. 10 seconds ago)
    past_time = time.time() - 10
    os.utime(file_path, (past_time, past_time))

    # Mock Loader to track calls
    mock_loader = MagicMock()
    mock_pipeline = Pipeline(state_file=state_file)
    mock_parser = MagicMock()
    # Mock parser to return a valid object so we hit the loader
    mock_trial = MagicMock()
    mock_trial.eudract_number = "2024-001"
    mock_parser.parse_trial.return_value = mock_trial
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    # --- Run 1: First Run (Should Process) ---
    run_silver(input_dir=str(bronze_dir), mode="FULL", parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Assert processed
    assert mock_parser.parse_trial.call_count == 1
    mock_parser.reset_mock()
    mock_loader.reset_mock()

    # --- Run 2: Immediate Re-run (Should Skip) ---
    # The file mtime hasn't changed.
    # The pipeline state should have recorded the last run time.
    run_silver(
        input_dir=str(bronze_dir),
        mode="FULL",  # Mode doesn't matter for the parsing decision, ideally
        parser=mock_parser,
        pipeline=mock_pipeline,
        loader=mock_loader,
    )

    # Assert SKIPPED
    assert mock_parser.parse_trial.call_count == 0, "Should have skipped unchanged file"

    # --- Run 3: Modify File (Should Process) ---
    # Update mtime to future to ensure it is strictly greater than the watermark
    # The watermark is the start_time of Run 1.
    future_time = time.time() + 5
    os.utime(file_path, (future_time, future_time))

    run_silver(input_dir=str(bronze_dir), mode="FULL", parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Assert processed
    assert mock_parser.parse_trial.call_count == 1
