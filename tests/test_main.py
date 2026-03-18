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

    # Mock components used in the orchestrator pipeline
    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    mock_downloader_instance.download_protocol_html.return_value = {"GB": "<html>GB1</html>"}

    mock_bronze_loader_class = mocker.patch("coreason_etl_euctr.bronze_loader.EpistemicBronzeLoaderTask")
    mock_bronze_loader_instance = mock_bronze_loader_class.return_value

    mock_parser_class = mocker.patch("coreason_etl_euctr.parser.EpistemicParserTask")
    mock_parser_instance = mock_parser_class.return_value
    mock_parser_instance.parse_html.return_value = {"parsed": "data"}

    mock_aggregator_class = mocker.patch("coreason_etl_euctr.aggregator.EpistemicGoldAggregatorTask")
    mock_aggregator_instance = mock_aggregator_class.return_value
    mock_aggregator_instance.aggregate.return_value = "mock_polars_df"

    mock_gold_loader_class = mocker.patch("coreason_etl_euctr.gold_loader.EpistemicGoldLoaderTask")
    mock_gold_loader_instance = mock_gold_loader_class.return_value

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(auto_mode=True)

    mock_logger_info.assert_any_call("Starting pipeline in AUTO mode.")
    mock_logger_info.assert_any_call("Discovered 2 EudraCT Numbers for processing.")
    mock_harvester_instance.harvest.assert_called_once()

    # Verify pipeline execution
    assert mock_downloader_instance.download_protocol_html.call_count == 2
    mock_bronze_loader_instance.load_html_blobs.assert_called()
    mock_parser_instance.parse_html.assert_called()
    mock_aggregator_instance.aggregate.assert_called_once()
    mock_gold_loader_instance.load_gold_dataframe.assert_called_once_with("mock_polars_df", write_disposition="merge")


def test_orchestrator_run_ids_file(mocker: MockerFixture, tmp_path: pytest.TempPathFactory) -> None:
    mock_logger_info = mocker.patch("coreason_etl_euctr.main.logger.info")
    mock_logger_warning = mocker.patch("coreason_etl_euctr.main.logger.warning")

    # Mock components used in the orchestrator pipeline
    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    # Simulate first ID having no HTML, and others having HTML
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

    mock_gold_loader_class = mocker.patch("coreason_etl_euctr.gold_loader.EpistemicGoldLoaderTask")
    mock_gold_loader_instance = mock_gold_loader_class.return_value

    # Create a temporary file with mock IDs
    file_path = tmp_path / "test_ids.txt"  # type: ignore[operator]
    file_path.write_text("ID_1\nID_2\nID_1\n \nID_3\n")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file=str(file_path))

    mock_logger_info.assert_any_call(f"Starting pipeline in IDS_FILE mode using: {file_path}")
    mock_logger_info.assert_any_call("Discovered 3 EudraCT Numbers for processing.")

    # Verify pipeline execution
    assert mock_downloader_instance.download_protocol_html.call_count == 3
    mock_logger_warning.assert_any_call("No HTML downloaded for ID_1, skipping to next ID.")
    assert mock_bronze_loader_instance.load_html_blobs.call_count == 2
    assert mock_parser_instance.parse_html.call_count == 2
    mock_aggregator_instance.aggregate.assert_called_once()
    mock_gold_loader_instance.load_gold_dataframe.assert_called_once_with("mock_polars_df", write_disposition="merge")


def test_orchestrator_run_empty_pipeline(mocker: MockerFixture, tmp_path: pytest.TempPathFactory) -> None:
    # Test case where downloaded data is empty and silver_data is empty
    mock_logger_warning = mocker.patch("coreason_etl_euctr.main.logger.warning")

    mock_downloader_class = mocker.patch("coreason_etl_euctr.downloader.EpistemicDownloaderTask")
    mock_downloader_instance = mock_downloader_class.return_value
    mock_downloader_instance.download_protocol_html.return_value = {}

    mock_bronze_loader_class = mocker.patch("coreason_etl_euctr.bronze_loader.EpistemicBronzeLoaderTask")
    mock_parser_class = mocker.patch("coreason_etl_euctr.parser.EpistemicParserTask")
    mock_aggregator_class = mocker.patch("coreason_etl_euctr.aggregator.EpistemicGoldAggregatorTask")
    mock_gold_loader_class = mocker.patch("coreason_etl_euctr.gold_loader.EpistemicGoldLoaderTask")

    file_path = tmp_path / "test_ids.txt"  # type: ignore[operator]
    file_path.write_text("ID_1\n")

    orchestrator = EpistemicPipelineOrchestratorTask()
    orchestrator.run(ids_file=str(file_path))

    mock_logger_warning.assert_any_call("No Silver data collected, skipping Gold Layer Aggregation and Loading.")
    mock_bronze_loader_class.return_value.load_html_blobs.assert_not_called()
    mock_parser_class.return_value.parse_html.assert_not_called()
    mock_aggregator_class.return_value.aggregate.assert_not_called()
    mock_gold_loader_class.return_value.load_gold_dataframe.assert_not_called()


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
