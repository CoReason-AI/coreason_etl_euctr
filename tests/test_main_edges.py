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
from unittest.mock import MagicMock

from coreason_etl_euctr.main import run_silver
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

    # We can't easily check the content of the stream passed to loader without inspecting the generator,
    # but we can verify that the orchestrator didn't crash and processed the valid ones.
    # The list passed to pipeline should contain 2 items.

    # However, pipeline is instantiated inside run_silver if not provided.
    # We didn't provide pipeline, so we can't inspect it directly.
    # But since we mock the loader, we know it got called.

    # Let's mock pipeline too to verify inputs
    mock_pipeline = MagicMock()
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
    Test iteration over a larger number of files to ensure no resource exhaustion
    (though purely mocked here, it ensures loop logic is sound).
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
    mock_pipeline.stage_data.return_value = iter(["header"])

    run_silver(input_dir=str(d), parser=mock_parser, pipeline=mock_pipeline, loader=mock_loader)

    # Verify all 50 were collected
    args, _ = mock_pipeline.stage_data.call_args
    assert len(args[0]) == 50
