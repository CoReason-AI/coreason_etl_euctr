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
from coreason_etl_euctr.redshift_loader import TextToBytesWrapper


@pytest.fixture  # type: ignore[misc]
def mock_clients() -> Generator[tuple[MagicMock, MagicMock, MagicMock, MagicMock], None, None]:
    with (
        patch("coreason_etl_euctr.bigquery_loader.bigquery") as mock_bq,
        patch("coreason_etl_euctr.bigquery_loader.storage") as mock_storage,
    ):
        mock_bq_client = MagicMock()
        mock_bq.Client.return_value = mock_bq_client

        mock_gcs_client = MagicMock()
        mock_storage.Client.return_value = mock_gcs_client

        yield mock_bq, mock_bq_client, mock_storage, mock_gcs_client


def test_upsert_deduplication_query_structure(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    """
    Verify that the MERGE query structure handles deduplication if we implement it.
    Currently checking if we construct the query correctly.
    """
    _, mock_bq_client, _, _ = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="b")
    loader.connect()

    data = io.StringIO("id,val\n1,v1\n1,v2")  # Duplicate ID 1

    # Mock table schema
    mock_table = MagicMock()
    f1 = MagicMock()
    f1.name = "id"
    f2 = MagicMock()
    f2.name = "val"
    mock_table.schema = [f1, f2]
    mock_bq_client.get_table.return_value = mock_table

    loader.upsert_stream(data, "my_table", conflict_keys=["id"])

    # Check the query sent to BigQuery
    mock_bq_client.query.assert_called()
    query = mock_bq_client.query.call_args[0][0]

    # Verify deduplication logic (QUALIFY ROW_NUMBER) is present
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY id" in query
    assert "MERGE" in query


def test_unicode_handling_in_upload(mock_clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, _, mock_gcs_client = mock_clients
    loader = BigQueryLoader(project_id="p", gcs_bucket="b")
    loader.connect()

    # Unicode string with Emoji and Chinese characters
    # 🧪 (4 bytes), 中 (3 bytes)
    content = "col1\nTest 🧪 中文"
    data = io.StringIO(content)

    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value.blob.return_value = mock_blob

    loader._upload_to_gcs(data)

    # Verify upload_from_file was called
    args, kwargs = mock_blob.upload_from_file.call_args
    uploaded_stream = args[0]

    # Read the stream that was passed to upload_from_file
    # It should be a TextToBytesWrapper wrapping the StringIO
    assert isinstance(uploaded_stream, TextToBytesWrapper)

    # Verify content
    uploaded_stream.read()
    expected_bytes = content.encode("utf-8")

    # Since TextToBytesWrapper reads from underlying stream which might be consumed?
    # Wait, TextToBytesWrapper reads on demand. upload_from_file reads it all.
    # We can't read it again easily unless we seek, but StringIO isn't seekable via wrapper easily?
    # Let's verify by reconstructing logic or just trusting the mock call if we could peek.
    # Actually, we can test TextToBytesWrapper separately or here.

    # Let's test TextToBytesWrapper behavior explicitly here
    wrapper = TextToBytesWrapper(io.StringIO(content))
    read_bytes = wrapper.read()
    assert read_bytes == expected_bytes


def test_large_stream_chunking() -> None:
    """Test TextToBytesWrapper with large data."""
    large_text = "a" * 10000 + "b" * 10000  # 20KB, larger than 8192 default chunk
    stream = io.StringIO(large_text)
    wrapper = TextToBytesWrapper(stream)

    read_data = b""
    while chunk := wrapper.read(4096):
        read_data += chunk

    assert read_data == large_text.encode("utf-8")
