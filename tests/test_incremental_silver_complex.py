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
    # Capture calls to stage_data to verify which files were processed
    mock_pipeline.stage_data.side_effect = lambda x: iter([f"header\nrow_{id(x)}\n"])

    mock_parser = MagicMock()

    # Return dummy trial for any input
    def side_effect_parse(content: str, url_source: str) -> EuTrial:
        return EuTrial(eudract_number=Path(url_source.replace("file://", "")).stem, url_source=url_source)

    mock_parser.parse_trial.side_effect = side_effect_parse
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    mock_loader = MagicMock()

    # Run with mocked time.time() = T0 + 50
    # Window is (1000, 1050].
    # Old (1000): Skipped (<= 1000).
    # New (1010): Processed.
    # Future (1100): Skipped (> 1050).

    with patch("time.time", return_value=T0 + 50):
        run_silver(input_dir=str(d), parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Verify Logic
    processed_sources = []
    for call_args in mock_parser.parse_trial.call_args_list:
        processed_sources.append(call_args.kwargs["url_source"])

    processed_filenames = [Path(s.replace("file://", "")).name for s in processed_sources]

    assert "old.html" not in processed_filenames
    assert "new.html" in processed_filenames
    assert "future.html" not in processed_filenames

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

    # Mock Parser to succeed
    mock_parser = MagicMock()
    mock_parser.parse_trial.return_value = EuTrial(eudract_number="123", url_source="s")
    mock_parser.parse_drugs.return_value = []
    mock_parser.parse_conditions.return_value = []

    # Run
    run_silver(input_dir=str(d), pipeline=mock_pipeline, loader=mock_loader, parser=mock_parser)

    # Verify Watermark NOT updated
    mock_pipeline.set_silver_watermark.assert_not_called()
    # Verify Rollback called
    mock_loader.rollback.assert_called_once()
