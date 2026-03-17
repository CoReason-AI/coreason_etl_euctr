# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import pytest
from pytest_mock import MockerFixture

from coreason_etl_euctr.main import EpistemicPipelineOrchestratorTask, main, parse_args


def test_parse_args_auto() -> None:
    args = parse_args(["--auto"])
    assert args.auto is True
    assert args.ids_file is None


def test_parse_args_ids_file() -> None:
    args = parse_args(["--ids-file", "test_ids.txt"])
    assert args.auto is False
    assert args.ids_file == "test_ids.txt"


def test_parse_args_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--auto", "--ids-file", "test_ids.txt"])


def test_parse_args_required() -> None:
    with pytest.raises(SystemExit):
        parse_args([])


def test_orchestrator_run_auto(mocker: MockerFixture) -> None:
    mock_logger = mocker.patch("coreason_etl_euctr.main.logger.info")
    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(auto_mode=True)
    mock_logger.assert_called_once_with("Starting pipeline in AUTO mode.")


def test_orchestrator_run_ids_file(mocker: MockerFixture) -> None:
    mock_logger = mocker.patch("coreason_etl_euctr.main.logger.info")
    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="test_ids.txt")
    mock_logger.assert_called_once_with("Starting pipeline in IDS_FILE mode using: test_ids.txt")


def test_orchestrator_run_none(mocker: MockerFixture) -> None:
    mock_logger = mocker.patch("coreason_etl_euctr.main.logger.warning")
    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run()
    mock_logger.assert_called_once_with("No execution mode specified. Exiting.")


def test_main_auto(mocker: MockerFixture) -> None:
    mock_run = mocker.patch("coreason_etl_euctr.main.EpistemicPipelineOrchestratorTask.run")
    main(["--auto"])
    mock_run.assert_called_once_with(auto_mode=True, ids_file=None)


def test_main_ids_file(mocker: MockerFixture) -> None:
    mock_run = mocker.patch("coreason_etl_euctr.main.EpistemicPipelineOrchestratorTask.run")
    main(["--ids-file", "test_ids.txt"])
    mock_run.assert_called_once_with(auto_mode=False, ids_file="test_ids.txt")
