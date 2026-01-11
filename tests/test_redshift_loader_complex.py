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
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from botocore.exceptions import ClientError
from coreason_etl_euctr.redshift_loader import ChainedStream, RedshiftLoader, TextToBytesWrapper


class TestChainedStreamComplex:
    def test_empty_first_chunk(self) -> None:
        """Test chaining when the first chunk is empty."""
        first = ""
        rest = io.StringIO("body")
        stream = ChainedStream(first, rest)
        assert stream.read() == "body"

    def test_empty_rest_stream(self) -> None:
        """Test chaining when the rest stream is empty."""
        first = "header"
        rest = io.StringIO("")
        stream = ChainedStream(first, rest)
        assert stream.read() == "header"

    def test_both_empty(self) -> None:
        """Test chaining when both are empty."""
        first = ""
        rest = io.StringIO("")
        stream = ChainedStream(first, rest)
        assert stream.read() == ""

    def test_read_boundary_exact(self) -> None:
        """Test reading exactly the size of the first chunk."""
        first = "12345"
        rest = io.StringIO("67890")
        stream = ChainedStream(first, rest)

        # Read exactly length of first
        chunk1 = stream.read(5)
        assert chunk1 == "12345"

        # Next read should come from rest
        chunk2 = stream.read(1)
        assert chunk2 == "6"

    def test_read_boundary_crossing(self) -> None:
        """Test reading across the boundary of first and rest."""
        first = "123"
        rest = io.StringIO("456")
        stream = ChainedStream(first, rest)

        # Read 4 chars (3 from first, 1 from rest)
        chunk = stream.read(4)
        assert chunk == "1234"
        assert stream.read() == "56"

    def test_read_tiny_chunks(self) -> None:
        """Test reading in very small chunks (1 byte)."""
        first = "AB"
        rest = io.StringIO("CD")
        stream = ChainedStream(first, rest)

        assert stream.read(1) == "A"
        assert stream.read(1) == "B"
        assert stream.read(1) == "C"
        assert stream.read(1) == "D"
        assert stream.read(1) == ""


class TestRedshiftLoaderComplex:
    @pytest.fixture  # type: ignore[misc]
    def mock_boto3(self) -> Any:
        with patch("coreason_etl_euctr.redshift_loader.boto3") as mock:
            yield mock

    @pytest.fixture  # type: ignore[misc]
    def mock_psycopg(self) -> Any:
        with patch("coreason_etl_euctr.redshift_loader.psycopg") as mock:
            mock.Error = psycopg.Error
            yield mock

    @pytest.fixture  # type: ignore[misc]
    def loader(self, mock_boto3: Any) -> RedshiftLoader:
        return RedshiftLoader(s3_bucket="test-bucket", s3_prefix="complex-prefix", region="us-east-1")

    def test_bulk_load_unicode_and_quotes(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        """Test bulk load with Unicode characters and quoted fields in CSV."""
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        # CSV content with Emoji, Unicode, and Quotes
        # Header: id, description
        # Row 1: 1, "Hello, World!"
        # Row 2: 2, "Café 🍵"
        csv_data = 'id,description\n1,"Hello, World!"\n2,"Café 🍵"'
        stream = io.StringIO(csv_data)

        loader.bulk_load_stream(stream, "test_table")

        # Verify upload called
        loader.s3_client.upload_fileobj.assert_called_once()

        # Inspect what was written? We can't easily inspect the BytesIO stream content in upload_fileobj call
        # unless we wrap it or mock side effect.
        # But we verify execution succeeded.

        # Verify SQL
        args, _ = cursor.execute.call_args
        sql = args[0]
        # Should detect columns correctly
        assert '("id", "description")' in sql

    def test_upsert_composite_keys(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        """Test upsert with multiple conflict keys."""
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        csv_data = "k1,k2,val\na,b,1"
        stream = io.StringIO(csv_data)

        conflict_keys = ["k1", "k2"]
        loader.upsert_stream(stream, "test_table", conflict_keys=conflict_keys)

        # Verify DELETE clause contains both keys
        # DELETE FROM target USING staging WHERE target."k1" = staging."k1" AND target."k2" = staging."k2"
        # Since logic uses a dict/generator, order might vary?
        # The implementation uses: `pk_conditions = " AND ".join(...)` iterating conflict_keys list.
        # List order is preserved.

        calls = [c[0][0] for c in cursor.execute.call_args_list]
        delete_call = next(c for c in calls if "DELETE FROM" in c)

        # staging table name is random, regex check or loose check
        assert 'test_table."k1" =' in delete_call
        assert 'test_table."k2" =' in delete_call
        assert " AND " in delete_call

    def test_s3_upload_failure(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        """Test that S3 upload failure propagates and doesn't run COPY."""
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        # Mock upload failure
        loader.s3_client.upload_fileobj.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "S3 Fail"}}, "PutObject"
        )

        with pytest.raises(ClientError):
            loader.bulk_load_stream(io.StringIO("h\nv"), "t")

        # Ensure COPY was NOT called
        cursor.execute.assert_not_called()

    def test_copy_failure_cleanup(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        """Test that S3 object is cleaned up even if COPY fails."""
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = psycopg.Error("COPY Syntax Error")

        with pytest.raises(psycopg.Error):
            loader.bulk_load_stream(io.StringIO("h\nv"), "t")

        # Ensure cleanup called
        loader.s3_client.delete_object.assert_called_once()

    def test_text_to_bytes_unicode_split(self, loader: RedshiftLoader) -> None:
        """
        Test TextToBytesWrapper when a multibyte character sits on the read chunk boundary.
        NOTE: TextToBytesWrapper reads from text stream (which yields chars), then encodes.
        It does NOT read bytes from text stream. So splitting characters is not an issue at source.
        However, if we read partial bytes from the buffer, we must ensure we handle it?
        TextToBytesWrapper.readinto fills a byte buffer.
        """
        # "€" is 3 bytes: b'\xe2\x82\xac'
        text = "A€B"
        stream = io.StringIO(text)
        wrapper = TextToBytesWrapper(stream)

        # Read 1 byte (A)
        b = bytearray(1)
        n = wrapper.readinto(b)
        assert n == 1
        assert b == b"A"

        # Read 1 byte (1st byte of €)
        b = bytearray(1)
        n = wrapper.readinto(b)
        assert n == 1
        assert b == b"\xe2"

        # Read 2 bytes (rest of €)
        b = bytearray(2)
        n = wrapper.readinto(b)
        assert n == 2
        assert b == b"\x82\xac"

        # Read 1 byte (B)
        b = bytearray(1)
        n = wrapper.readinto(b)
        assert n == 1
        assert b == b"B"
