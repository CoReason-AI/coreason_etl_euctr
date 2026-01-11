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


class TestTextToBytesWrapper:
    def test_read_all(self) -> None:
        text = "Hello, World! €"
        stream = io.StringIO(text)
        wrapper = TextToBytesWrapper(stream)

        assert wrapper.read() == text.encode("utf-8")

    def test_read_chunked(self) -> None:
        text = "1234567890"
        stream = io.StringIO(text)
        wrapper = TextToBytesWrapper(stream)

        chunk1 = wrapper.read(5)
        assert chunk1 == b"12345"
        chunk2 = wrapper.read(5)
        assert chunk2 == b"67890"

    def test_readable(self) -> None:
        wrapper = TextToBytesWrapper(io.StringIO("test"))
        assert wrapper.readable() is True


class TestChainedStream:
    def test_read_chained(self) -> None:
        first = "Header\n"
        rest = io.StringIO("Body")
        stream = ChainedStream(first, rest)
        assert stream.read() == "Header\nBody"

    def test_read_chained_chunked(self) -> None:
        first = "123"
        rest = io.StringIO("456")
        stream = ChainedStream(first, rest)
        assert stream.read(2) == "12"
        assert stream.read(2) == "34"
        assert stream.read(2) == "56"
        assert stream.read() == ""


class TestRedshiftLoader:
    @pytest.fixture
    def mock_boto3(self) -> Any:
        with patch("coreason_etl_euctr.redshift_loader.boto3") as mock:
            yield mock

    @pytest.fixture
    def mock_psycopg(self) -> Any:
        with patch("coreason_etl_euctr.redshift_loader.psycopg") as mock:
            mock.Error = psycopg.Error
            yield mock

    @pytest.fixture
    def loader(self, mock_boto3: Any) -> RedshiftLoader:
        return RedshiftLoader(s3_bucket="test-bucket", s3_prefix="prefix", region="us-east-1")

    def test_connect(self, loader: RedshiftLoader, mock_psycopg: Any) -> None:
        loader.connect()
        mock_psycopg.connect.assert_called_once()
        assert loader.conn is not None

    def test_connect_fail(self, loader: RedshiftLoader, mock_psycopg: Any) -> None:
        # Since we assigned mock.Error = psycopg.Error, we can catch it.
        # But we also need side_effect to raise an instance of it.
        mock_psycopg.connect.side_effect = psycopg.Error("Connection Failed")
        with pytest.raises(psycopg.Error):
            loader.connect()

    def test_close(self, loader: RedshiftLoader, mock_psycopg: Any) -> None:
        conn = MagicMock()
        loader.conn = conn
        loader.close()
        conn.close.assert_called_once()
        assert loader.conn is None

    def test_prepare_schema(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.prepare_schema()
        # Verify CREATE TABLE statements
        cursor = loader.conn.cursor.return_value.__enter__.return_value
        assert cursor.execute.call_count >= 3

    def test_prepare_schema_not_connected(self, loader: RedshiftLoader) -> None:
        with pytest.raises(RuntimeError, match="Database not connected"):
            loader.prepare_schema()

    def test_prepare_schema_fail(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.conn.cursor.return_value.__enter__.return_value.execute.side_effect = psycopg.Error("Schema Error")
        with pytest.raises(psycopg.Error):
            loader.prepare_schema()
        loader.conn.rollback.assert_called_once()

    def test_bulk_load_stream(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        # Create a mock session for credentials fallback
        mock_session = mock_boto3.Session.return_value
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value.access_key = "AK"
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value.secret_key = "SK"
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value.token = "TOK"

        csv_data = "col1,col2\nval1,val2"
        stream = io.StringIO(csv_data)

        loader.bulk_load_stream(stream, "test_table")

        # Verify S3 Upload
        loader.s3_client.upload_fileobj.assert_called_once()

        # Verify COPY command
        args, _ = cursor.execute.call_args
        sql = args[0]
        assert "COPY test_table" in sql
        assert "FROM 's3://test-bucket/prefix/" in sql
        assert "CREDENTIALS 'aws_access_key_id=AK" in sql
        assert "FORMAT AS CSV IGNOREHEADER 1" in sql
        assert '("col1", "col2")' in sql

    def test_bulk_load_iam_role(self, mock_boto3: Any, mock_psycopg: Any) -> None:
        loader = RedshiftLoader("bkt", iam_role="arn:aws:iam::123:role/RedshiftRole")
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        csv_data = "col1\nval1"
        stream = io.StringIO(csv_data)

        loader.bulk_load_stream(stream, "test_table")

        args, _ = cursor.execute.call_args
        sql = args[0]
        assert "IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'" in sql

    def test_bulk_load_not_connected(self, loader: RedshiftLoader) -> None:
        with pytest.raises(RuntimeError, match="Database not connected"):
            loader.bulk_load_stream(io.StringIO(""), "t")

    def test_bulk_load_empty(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.bulk_load_stream(io.StringIO(""), "t")
        loader.s3_client.upload_fileobj.assert_not_called()

    def test_bulk_load_fail(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        loader.conn = MagicMock()
        loader.conn.cursor.return_value.__enter__.return_value.execute.side_effect = psycopg.Error("Copy Error")

        mock_session = mock_boto3.Session.return_value
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value.access_key = "AK"

        with pytest.raises(psycopg.Error):
            loader.bulk_load_stream(io.StringIO("h\nv"), "t")

    def test_upsert_stream(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value

        # Mock Session
        mock_boto3.Session.return_value.get_credentials.return_value.get_frozen_credentials.return_value.access_key = "AK"

        csv_data = "id,name\n1,foo"
        stream = io.StringIO(csv_data)

        loader.upsert_stream(stream, "test_table", conflict_keys=["id"])

        # Verify sequence: CREATE TEMP -> COPY -> DELETE -> INSERT -> DROP
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("CREATE TEMP TABLE" in c for c in calls)
        assert any("COPY" in c and "_staging_" in c for c in calls)
        assert any("DELETE FROM test_table USING" in c for c in calls)
        assert any("INSERT INTO test_table" in c for c in calls)
        assert any("DROP TABLE" in c for c in calls)

    def test_upsert_not_connected(self, loader: RedshiftLoader) -> None:
        with pytest.raises(RuntimeError, match="Database not connected"):
            loader.upsert_stream(io.StringIO(""), "t", [])

    def test_upsert_empty(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.upsert_stream(io.StringIO(""), "t", [])
        loader.s3_client.upload_fileobj.assert_not_called()

    def test_upsert_fail(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        loader.conn = MagicMock()
        cursor = loader.conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = psycopg.Error("Upsert Fail")

        mock_session = mock_boto3.Session.return_value
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value.access_key = "AK"

        with pytest.raises(psycopg.Error):
            loader.upsert_stream(io.StringIO("id\n1"), "t", ["id"])

    def test_truncate_tables(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.truncate_tables(["t1", "t2"])
        cursor = loader.conn.cursor.return_value.__enter__.return_value
        assert cursor.execute.call_count == 2

    def test_truncate_not_connected(self, loader: RedshiftLoader) -> None:
        with pytest.raises(RuntimeError):
            loader.truncate_tables(["t1"])

    def test_truncate_fail(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.conn.cursor.return_value.__enter__.return_value.execute.side_effect = psycopg.Error("Err")
        with pytest.raises(psycopg.Error):
            loader.truncate_tables(["t1"])

    def test_transactions(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        loader.commit()
        loader.conn.commit.assert_called_once()
        loader.rollback()
        loader.conn.rollback.assert_called_once()

    def test_s3_cleanup(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        stream = io.StringIO("header\nval")
        loader.bulk_load_stream(stream, "t")

        loader.s3_client.delete_object.assert_called_once()

    def test_s3_cleanup_fail(self, loader: RedshiftLoader) -> None:
        loader.conn = MagicMock()
        # Mock S3 delete to raise error
        loader.s3_client.delete_object.side_effect = Exception("S3 Fail")

        # Should not raise exception (logged only)
        stream = io.StringIO("header\nval")
        loader.bulk_load_stream(stream, "t")
        loader.s3_client.delete_object.assert_called_once()

    def test_build_copy_no_creds(self, loader: RedshiftLoader, mock_boto3: Any) -> None:
        # Mock Session return None for credentials
        mock_session = mock_boto3.Session.return_value
        mock_session.get_credentials.return_value = None

        with pytest.raises(ValueError, match="No IAM Role provided"):
            loader._build_copy_command("t", "s3://")
