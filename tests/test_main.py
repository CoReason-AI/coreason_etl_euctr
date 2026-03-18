# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from coreason_etl_euctr.main import EpistemicPipelineOrchestratorTask, main, parse_args


@patch("coreason_etl_euctr.main.generate_deterministic_hash", return_value="new_hash")
@patch("coreason_etl_euctr.main.EpistemicStateManagerTask")
@patch("coreason_etl_euctr.harvester.EpistemicHarvesterTask")
def test_orchestrator_auto_mode(
    mock_harvester: MagicMock, mock_state_manager: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("coreason_etl_euctr.main.logger.warning")

    mock_state_instance = mock_state_manager.return_value
    mock_state_instance.last_run_timestamp = "2024-01-01T12:00:00Z"
    mock_state_instance.get_hash.return_value = "old_hash"

    mock_harvester_instance = mock_harvester.return_value
    mock_harvester_instance.harvest.return_value = ["ID_1"]

    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    mock_downloader_instance.download_protocol_html.return_value = {"GB": "<html>GB1</html>"}

    mocker.patch("coreason_etl_euctr.bronze_loader.EpistemicBronzeLoaderTask")

    mock_parser_class = mocker.patch("coreason_etl_euctr.parser.EpistemicParserTask")
    mock_parser_instance = mock_parser_class.return_value
    mock_parser_instance.parse_html.return_value = {"parsed": "data"}

    mock_aggregator_class = mocker.patch("coreason_etl_euctr.aggregator.EpistemicGoldAggregatorTask")
    mock_aggregator_instance = mock_aggregator_class.return_value
    mock_aggregator_instance.aggregate.return_value = "mock_polars_df"

    mocker.patch("coreason_etl_euctr.gold_loader.EpistemicGoldLoaderTask")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(auto_mode=True)

    mock_harvester_instance.harvest.assert_called_once_with(date_from="2024-01-01")
    assert mock_state_instance.last_run_timestamp != "2024-01-01T12:00:00Z"


def test_orchestrator_run_ids_file(mocker: MockerFixture, tmp_path: pytest.TempPathFactory) -> None:
    mock_logger_warning = mocker.patch("coreason_etl_euctr.main.logger.warning")
    mock_logger_info = mocker.patch("coreason_etl_euctr.main.logger.info")
    mocker.patch("coreason_etl_euctr.main.generate_deterministic_hash", side_effect=["hash1", "hash2"])

    mock_state_class = mocker.patch("coreason_etl_euctr.main.EpistemicStateManagerTask")
    mock_state_instance = mock_state_class.return_value
    mock_state_instance.get_hash.side_effect = ["different_hash", "different_hash2"]

    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    mock_downloader_instance.download_protocol_html.side_effect = [
        {},
        {"GB": "<html>GB2</html>"},
        {"DE": "<html>DE3</html>"},
    ]

    mock_bronze_loader_class = mocker.patch("coreason_etl_euctr.bronze_loader.EpistemicBronzeLoaderTask")
    mock_bronze_loader_instance = mock_bronze_loader_class.return_value

    mock_parser_class = mocker.patch("coreason_etl_euctr.parser.EpistemicParserTask")
    mock_parser_instance = mock_parser_class.return_value
    mock_parser_instance.parse_html.return_value = {"parsed": "data"}

    mock_aggregator_class = mocker.patch("coreason_etl_euctr.aggregator.EpistemicGoldAggregatorTask")
    mock_aggregator_instance = mock_aggregator_class.return_value
    mock_aggregator_instance.aggregate.return_value = "mock_polars_df"

    mocker.patch("coreason_etl_euctr.gold_loader.EpistemicGoldLoaderTask")

    file_path = tmp_path / "test_ids.txt"  # type: ignore[operator]
    file_path.write_text("ID_1\nID_2\nID_1\n \nID_3\n")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file=str(file_path))

    mock_logger_info.assert_any_call(f"Starting pipeline in IDS_FILE mode using: {file_path}")
    assert mock_downloader_instance.download_protocol_html.call_count == 3
    mock_logger_warning.assert_any_call("No HTML downloaded for ID_1, skipping to next ID.")
    assert mock_bronze_loader_instance.load_html_blobs.call_count == 2


def test_orchestrator_run_ids_file_error(mocker: MockerFixture) -> None:
    mock_logger_error = mocker.patch("coreason_etl_euctr.main.logger.error")

    original_open = open

    def mocked_open(*args: Any, **kwargs: Any) -> Any:
        if args[0] == "some_file.txt":
            raise Exception("mocked error")
        return original_open(*args, **kwargs)

    mocker.patch("builtins.open", side_effect=mocked_open)

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="some_file.txt")

    mock_logger_error.assert_called_once_with("Error reading file some_file.txt: mocked error")


def test_orchestrator_run_ids_file_not_found(mocker: MockerFixture) -> None:
    mock_logger_error = mocker.patch("coreason_etl_euctr.main.logger.error")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="non_existent_file.txt")

    mock_logger_error.assert_called_once_with("File not found: non_existent_file.txt")


def test_orchestrator_no_mode(mocker: MockerFixture) -> None:
    mock_logger_warning = mocker.patch("coreason_etl_euctr.main.logger.warning")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run()

    mock_logger_warning.assert_called_once_with("No execution mode specified. Exiting.")


def test_parse_args_auto() -> None:
    args = parse_args(["--auto"])
    assert args.auto is True
    assert args.ids_file is None


def test_parse_args_ids_file() -> None:
    args = parse_args(["--ids-file", "test.txt"])
    assert args.auto is False
    assert args.ids_file == "test.txt"


def test_main_auto(mocker: MockerFixture) -> None:
    mock_orchestrator_class = mocker.patch("coreason_etl_euctr.main.EpistemicPipelineOrchestratorTask")
    mock_instance = mock_orchestrator_class.return_value

    main(["--auto"])

    mock_instance.run.assert_called_once_with(auto_mode=True, ids_file=None)


def test_main_ids_file(mocker: MockerFixture) -> None:
    mock_orchestrator_class = mocker.patch("coreason_etl_euctr.main.EpistemicPipelineOrchestratorTask")
    mock_instance = mock_orchestrator_class.return_value

    main(["--ids-file", "test.txt"])

    mock_instance.run.assert_called_once_with(auto_mode=False, ids_file="test.txt")


def test_idempotency_skip(mocker: MockerFixture) -> None:
    mocker.patch("coreason_etl_euctr.main.logger.info")
    mocker.patch("coreason_etl_euctr.main.generate_deterministic_hash", return_value="same_hash")

    mock_state_class = mocker.patch("coreason_etl_euctr.main.EpistemicStateManagerTask")
    mock_state_instance = mock_state_class.return_value
    mock_state_instance.get_hash.return_value = "same_hash"

    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    mock_downloader_instance.download_protocol_html.return_value = {"GB": "<html>GB1</html>"}

    mock_bronze_loader_class = mocker.patch("coreason_etl_euctr.bronze_loader.EpistemicBronzeLoaderTask")
    mock_bronze_loader_instance = mock_bronze_loader_class.return_value

    mock_parser_class = mocker.patch("coreason_etl_euctr.parser.EpistemicParserTask")
    mock_parser_instance = mock_parser_class.return_value
    mock_parser_instance.parse_html.return_value = {"parsed": "data"}

    # Mock file read to return 1 ID
    original_open = open

    def mocked_open(*args: Any, **kwargs: Any) -> Any:
        if args[0] == "test.txt":
            from io import StringIO

            return StringIO("ID_1")
        return original_open(*args, **kwargs)

    mocker.patch("builtins.open", side_effect=mocked_open)

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file="test.txt")

    mock_bronze_loader_instance.load_html_blobs.assert_not_called()
