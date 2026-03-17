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
    mock_logger_info = mocker.patch("coreason_etl_euctr.main.logger.info")
    mock_harvester_class = mocker.patch("coreason_etl_euctr.harvester.EpistemicHarvesterTask")
    mock_harvester_instance = mock_harvester_class.return_value
    mock_harvester_instance.harvest.return_value = ["ID_1", "ID_2"]

    # We also need to patch the import inside the method to return our mock
    # A cleaner way is to mock sys.modules or patch the harvester module directly
    # However, since the import is `from coreason_etl_euctr.harvester import EpistemicHarvesterTask`
    # inside the function, patching `coreason_etl_euctr.harvester.EpistemicHarvesterTask` will work

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(auto_mode=True)

    mock_logger_info.assert_any_call("Starting pipeline in AUTO mode.")
    mock_logger_info.assert_any_call("Discovered 2 EudraCT Numbers for processing.")
    mock_harvester_instance.harvest.assert_called_once()


def test_orchestrator_run_ids_file(mocker: MockerFixture, tmp_path: pytest.TempPathFactory) -> None:
    mock_logger_info = mocker.patch("coreason_etl_euctr.main.logger.info")

    # Create a temporary file with mock IDs
    file_path = tmp_path / "test_ids.txt"  # type: ignore[operator]
    file_path.write_text("ID_1\nID_2\nID_1\n \nID_3\n")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file=str(file_path))

    mock_logger_info.assert_any_call(f"Starting pipeline in IDS_FILE mode using: {file_path}")
    mock_logger_info.assert_any_call("Discovered 3 EudraCT Numbers for processing.")


def test_orchestrator_run_ids_file_not_found(mocker: MockerFixture) -> None:
    mock_logger_error = mocker.patch("coreason_etl_euctr.main.logger.error")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="nonexistent_file.txt")

    mock_logger_error.assert_called_once_with("File not found: nonexistent_file.txt")


def test_orchestrator_run_ids_file_error(mocker: MockerFixture) -> None:
    mock_logger_error = mocker.patch("coreason_etl_euctr.main.logger.error")
    mocker.patch("builtins.open", side_effect=Exception("mocked error"))

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="some_file.txt")

    mock_logger_error.assert_called_once_with("Error reading file some_file.txt: mocked error")


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
