# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from dlt.common.pipeline import LoadInfo
from pytest_mock import MockerFixture

from coreason_etl_euctr.bronze_loader import EpistemicBronzeLoaderTask


def test_bronze_loader_init() -> None:
    loader = EpistemicBronzeLoaderTask(pipeline_name="test_pipeline", destination="duckdb", dataset_name="test_dataset")
    assert loader.pipeline_name == "test_pipeline"
    assert loader.destination == "duckdb"
    assert loader.dataset_name == "test_dataset"


def test_bronze_loader_load_html_blobs(mocker: MockerFixture) -> None:
    loader = EpistemicBronzeLoaderTask()

    # Mock the dlt pipeline object and its run method
    mock_pipeline_run = mocker.patch("dlt.pipeline")
    mock_pipeline_instance = mock_pipeline_run.return_value
    mock_load_info = mocker.MagicMock(spec=LoadInfo)
    mock_pipeline_instance.run.return_value = mock_load_info

    eudract_id = "2020-000000-00"
    downloaded_htmls = {"GB": "<html>GB Protocol</html>", "DE": "<html>DE Protocol</html>"}

    load_info = loader.load_html_blobs(eudract_id, downloaded_htmls)

    # Verify pipeline was called with correct arguments
    mock_pipeline_run.assert_called_once_with(
        pipeline_name=loader.pipeline_name,
        destination=loader.destination,
        dataset_name=loader.dataset_name,
    )

    # Verify run method was called with correct list of dicts
    expected_data_to_load = [
        {"eudract_id": eudract_id, "country_code": "GB", "raw_html": "<html>GB Protocol</html>"},
        {"eudract_id": eudract_id, "country_code": "DE", "raw_html": "<html>DE Protocol</html>"},
    ]
    mock_pipeline_instance.run.assert_called_once_with(
        expected_data_to_load,
        table_name="raw_html_blobs",
        write_disposition="append",
    )

    assert load_info == mock_load_info


def test_bronze_loader_empty_html_blobs(mocker: MockerFixture) -> None:
    loader = EpistemicBronzeLoaderTask()

    mock_pipeline_run = mocker.patch("dlt.pipeline")
    mock_pipeline_instance = mock_pipeline_run.return_value
    mock_load_info = mocker.MagicMock(spec=LoadInfo)
    mock_pipeline_instance.run.return_value = mock_load_info

    eudract_id = "2020-000000-00"
    downloaded_htmls: dict[str, str] = {}

    load_info = loader.load_html_blobs(eudract_id, downloaded_htmls)

    # Run method should still be called, but with empty list
    mock_pipeline_instance.run.assert_called_once_with([])

    assert load_info == mock_load_info


def test_bronze_loader_read_all_html_blobs_success(mocker: MockerFixture) -> None:
    loader = EpistemicBronzeLoaderTask(
        pipeline_name="test_read_pipeline", destination="duckdb", dataset_name="test_bronze"
    )

    mock_duckdb_connect = mocker.patch("duckdb.connect")
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()

    # Setup context manager
    mock_duckdb_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_cursor

    # Mock data returned from database
    mock_cursor.fetchall.return_value = [
        ("ID-1", "GB", "<html>GB 1</html>"),
        ("ID-1", "DE", "<html>DE 1</html>"),
        ("ID-2", "GB", "<html>GB 2</html>"),
    ]

    result = loader.read_all_html_blobs()

    mock_duckdb_connect.assert_called_once_with("test_read_pipeline.duckdb", read_only=True)
    mock_conn.execute.assert_called_once_with(
        "SELECT eudract_id, country_code, raw_html FROM test_bronze.raw_html_blobs"
    )

    assert len(result) == 2
    assert "ID-1" in result
    assert "ID-2" in result
    assert result["ID-1"]["GB"] == "<html>GB 1</html>"
    assert result["ID-1"]["DE"] == "<html>DE 1</html>"
    assert result["ID-2"]["GB"] == "<html>GB 2</html>"


def test_bronze_loader_read_all_html_blobs_not_duckdb(mocker: MockerFixture) -> None:
    loader = EpistemicBronzeLoaderTask(destination="postgres")

    mock_logger_error = mocker.patch("coreason_etl_euctr.bronze_loader.logger.error")

    result = loader.read_all_html_blobs()

    assert result == {}
    mock_logger_error.assert_called_once()
    assert "only implemented for duckdb destination" in mock_logger_error.call_args[0][0]


def test_bronze_loader_read_all_html_blobs_duckdb_error(mocker: MockerFixture) -> None:
    import duckdb

    loader = EpistemicBronzeLoaderTask()

    mock_duckdb_connect = mocker.patch("duckdb.connect")
    mock_duckdb_connect.side_effect = duckdb.Error("Database file not found")

    mock_logger_error = mocker.patch("coreason_etl_euctr.bronze_loader.logger.error")

    result = loader.read_all_html_blobs()

    assert result == {}
    mock_logger_error.assert_called_once()
    assert "DuckDB error" in mock_logger_error.call_args[0][0]


def test_bronze_loader_read_all_html_blobs_general_error(mocker: MockerFixture) -> None:
    loader = EpistemicBronzeLoaderTask()

    mock_duckdb_connect = mocker.patch("duckdb.connect")
    mock_duckdb_connect.side_effect = Exception("Some general error")

    mock_logger_error = mocker.patch("coreason_etl_euctr.bronze_loader.logger.error")

    result = loader.read_all_html_blobs()

    assert result == {}
    mock_logger_error.assert_called_once()
    assert "Unexpected error" in mock_logger_error.call_args[0][0]
