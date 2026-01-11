# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import io
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.bigquery_loader import BigQueryLoader
from google.api_core.exceptions import NotFound


@pytest.fixture  # type: ignore[misc]
def mock_clients() -> Generator[tuple[MagicMock, MagicMock, MagicMock, MagicMock], None, None]:
    with (
        patch("coreason_etl_euctr.bigquery_loader.bigquery") as mock_bq,
        patch("coreason_etl_euctr.bigquery_loader.storage") as mock_storage,
    ):
        # Configure BQ Client
        mock_bq_client = MagicMock()
        mock_bq.Client.return_value = mock_bq_client

        # Configure GCS Client
        mock_gcs_client = MagicMock()
        mock_storage.Client.return_value = mock_gcs_client

        yield mock_bq, mock_bq_client, mock_storage, mock_gcs_client


def test_bigquery_loader_initialization() -> None:
    loader = BigQueryLoader(project_id="test-proj", gcs_bucket="test-bucket")
    assert loader.project_id == "test-proj"
    assert loader.gcs_bucket == "test-bucket"
    assert loader.dataset_id == "eu_ctr"


def test_connect(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    mock_bq, mock_bq_client, mock_storage, mock_gcs_client = mock_clients

    loader = BigQueryLoader(project_id="test-proj")
    loader.connect()

    mock_bq.Client.assert_called_with(project="test-proj", location="US")
    mock_storage.Client.assert_called_with(project="test-proj")
    assert loader.bq_client == mock_bq_client
    assert loader.gcs_client == mock_gcs_client


def test_connect_failure(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    mock_bq, _, _, _ = mock_clients
    mock_bq.Client.side_effect = RuntimeError("Connection Error")

    loader = BigQueryLoader(project_id="p")

    with pytest.raises(RuntimeError):
        loader.connect()


def test_close(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    loader.close()

    mock_bq_client.close.assert_called_once()
    mock_gcs_client.close.assert_called_once()
    assert loader.bq_client is None
    assert loader.gcs_client is None


def test_prepare_schema_not_connected() -> None:
    loader = BigQueryLoader(project_id="p")
    with pytest.raises(RuntimeError, match="Database not connected"):
        loader.prepare_schema()


def test_prepare_schema_creates_dataset(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    mock_bq, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    # Simulate Dataset NotFound
    mock_bq_client.get_dataset.side_effect = NotFound("Not found")

    loader.prepare_schema()

    # Verify create_dataset called
    assert mock_bq_client.create_dataset.called


def test_prepare_schema_creates_tables(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    mock_bq, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    # Simulate Dataset Exists, Table NotFound
    mock_bq_client.get_dataset.return_value = MagicMock()
    mock_bq_client.get_table.side_effect = NotFound("Table Not Found")

    loader.prepare_schema()

    # Should attempt to create 3 tables
    assert mock_bq_client.create_table.call_count == 3


def test_upload_to_gcs_no_client() -> None:
    loader = BigQueryLoader(project_id="p")
    # Manually unset clients to be safe
    loader.gcs_client = None

    with pytest.raises(RuntimeError):
        loader._upload_to_gcs(io.StringIO("d"))


def test_delete_gcs_object(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    mock_bucket = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket

    loader._delete_gcs_object("gs://bucket/key.csv")

    mock_bucket.delete_blob.assert_called_with("key.csv")


def test_delete_gcs_object_error(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    mock_bucket = MagicMock()
    mock_bucket.delete_blob.side_effect = Exception("Delete failed")
    mock_gcs_client.bucket.return_value = mock_bucket

    # Should log warning but not raise
    loader._delete_gcs_object("gs://bucket/key.csv")


def test_bulk_load_stream_success(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="bucket")
    loader.connect()

    data = io.StringIO("col1,col2\nval1,val2")

    # Mock Load Job
    mock_job = MagicMock()
    mock_job.output_rows = 100
    mock_bq_client.load_table_from_uri.return_value = mock_job

    loader.bulk_load_stream(data, "eu_trials")

    # Verify Upload
    mock_gcs_client.bucket.assert_called_with("bucket")
    # Verify Load
    mock_bq_client.load_table_from_uri.assert_called_once()
    # Verify Job Wait
    mock_job.result.assert_called_once()


def test_bulk_load_stream_empty(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    data = io.StringIO("")  # Empty
    loader.bulk_load_stream(data, "eu_trials")

    mock_bq_client.load_table_from_uri.assert_not_called()


def test_bulk_load_stream_error(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="b")
    loader.connect()

    data = io.StringIO("h\nd")
    mock_bq_client.load_table_from_uri.side_effect = Exception("Load Failed")

    with pytest.raises(Exception, match="Load Failed"):
        loader.bulk_load_stream(data, "t")


def test_bulk_load_not_connected() -> None:
    loader = BigQueryLoader(project_id="p")
    with pytest.raises(RuntimeError):
        loader.bulk_load_stream(io.StringIO("d"), "t")


def test_upsert_stream_success(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="bucket")
    loader.connect()

    data = io.StringIO("id,val\n1,a")

    # Mock get_table to return schema
    mock_table = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "id"
    mock_table.schema = [mock_field]
    mock_bq_client.get_table.return_value = mock_table

    # Mock Load Job
    mock_load_job = MagicMock()
    mock_bq_client.load_table_from_uri.return_value = mock_load_job

    # Mock Query Job (MERGE)
    mock_query_job = MagicMock()
    mock_bq_client.query.return_value = mock_query_job

    loader.upsert_stream(data, "eu_trials", conflict_keys=["id"])

    # Verify:
    # 1. Create Staging Table
    assert mock_bq_client.create_table.called
    # 2. Load to Staging
    mock_bq_client.load_table_from_uri.assert_called()
    # 3. MERGE Query
    mock_bq_client.query.assert_called()
    assert "MERGE" in mock_bq_client.query.call_args[0][0]
    # 4. Cleanup Staging
    mock_bq_client.delete_table.assert_called()


def test_upsert_stream_empty(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    loader.upsert_stream(io.StringIO(""), "t", [])
    mock_bq_client.create_table.assert_not_called()


def test_upsert_stream_error(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="b")
    loader.connect()

    data = io.StringIO("h\nd")
    mock_bq_client.get_table.side_effect = Exception("Upsert Fail")

    with pytest.raises(Exception, match="Upsert Fail"):
        loader.upsert_stream(data, "t", ["id"])

    # Should still try to cleanup
    mock_bq_client.delete_table.assert_called()


def test_upsert_not_connected() -> None:
    loader = BigQueryLoader(project_id="p")
    with pytest.raises(RuntimeError):
        loader.upsert_stream(io.StringIO("d"), "t", ["k"])


def test_truncate_tables(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    loader.truncate_tables(["eu_trials"])

    mock_bq_client.query.assert_called()
    assert "TRUNCATE TABLE" in mock_bq_client.query.call_args[0][0]


def test_truncate_tables_error(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p")
    loader.connect()

    mock_bq_client.query.side_effect = Exception("Truncate Fail")

    with pytest.raises(Exception, match="Truncate Fail"):
        loader.truncate_tables(["t"])


def test_truncate_not_connected() -> None:
    loader = BigQueryLoader(project_id="p")
    with pytest.raises(RuntimeError):
        loader.truncate_tables(["t"])


def test_commit_rollback(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    loader = BigQueryLoader(project_id="p")
    # Should do nothing and not raise
    loader.commit()
    loader.rollback()
