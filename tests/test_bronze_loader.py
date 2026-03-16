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
