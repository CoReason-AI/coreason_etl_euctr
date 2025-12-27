# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import os
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.postgres_loader import PostgresLoader


@pytest.fixture
def loader() -> PostgresLoader:  # type: ignore[misc]
    # Use dummy DSN to avoid env var lookups unless testing that
    return PostgresLoader(dsn="postgresql://user:pass@localhost:5432/db")


def test_init_env_vars() -> None:
    """Test initialization with environment variables."""
    with patch.dict(
        os.environ,
        {"DB_USER": "u", "DB_PASS": "p", "DB_HOST": "h", "DB_PORT": "5432", "DB_NAME": "d", "DATABASE_URL": ""},
    ):
        _loader = PostgresLoader(dsn=None)
        assert _loader.dsn == "postgresql://u:p@h:5432/d"


def test_connect_success(loader: PostgresLoader) -> None:
    with patch("psycopg.connect") as mock_connect:
        loader.connect()
        mock_connect.assert_called_once_with(loader.dsn)
        assert loader.conn is not None


def test_prepare_schema(loader: PostgresLoader) -> None:
    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        loader.prepare_schema()

        # Check if SQL was executed
        assert mock_cursor.execute.called
        # Verify commit
        assert mock_conn.commit.called


def test_bulk_load_stream(loader: PostgresLoader) -> None:
    csv_data = StringIO("col1,col2\nval1,val2")

    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy

        loader.bulk_load_stream("test_table", csv_data, ["col1", "col2"])

        # Verify copy command
        args, _ = mock_cursor.copy.call_args
        assert "COPY test_table (col1,col2) FROM STDIN" in args[0]

        # Verify data write
        assert mock_copy.write.called
        assert mock_conn.commit.called


def test_bulk_load_stream_rollback_on_error(loader: PostgresLoader) -> None:
    csv_data = StringIO("data")

    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        # Simulate error during copy
        mock_cursor.copy.side_effect = ValueError("Copy Failed")

        with pytest.raises(ValueError):  # noqa: B017
            loader.bulk_load_stream("test_table", csv_data, ["col1"])

        assert mock_conn.rollback.called


def test_upsert_stream(loader: PostgresLoader) -> None:
    csv_data = StringIO("id,val\n1,a")

    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy

        loader.upsert_stream("test_table", csv_data, ["id", "val"], ["id"])

        # Verify sequence: Create Temp -> Copy -> Insert/Conflict
        calls = mock_cursor.execute.call_args_list
        assert "CREATE TEMP TABLE" in calls[0][0][0]
        assert "INSERT INTO test_table" in calls[1][0][0]
        assert "ON CONFLICT (id)" in calls[1][0][0]

        assert mock_conn.commit.called


def test_upsert_stream_no_update_cols(loader: PostgresLoader) -> None:
    """Test upsert where all columns are conflict keys (DO NOTHING)."""
    csv_data = StringIO("id\n1")

    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = MagicMock()

        loader.upsert_stream("test_table", csv_data, ["id"], ["id"])

        # Check for DO NOTHING in SQL
        # The second call to execute is the merge
        merge_call = mock_cursor.execute.call_args_list[1]
        assert "DO NOTHING" in merge_call[0][0]


def test_upsert_rollback_on_error(loader: PostgresLoader) -> None:
    csv_data = StringIO("data")
    with patch("psycopg.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.execute.side_effect = ValueError("Upsert Fail")

        with pytest.raises(ValueError):  # noqa: B017
            loader.upsert_stream("t", csv_data, ["a"], ["a"])

        assert mock_conn.rollback.called


def test_close(loader: PostgresLoader) -> None:
    loader.conn = MagicMock()
    loader.conn.closed = False  # Explicitly set closed to False
    loader.close()
    assert loader.conn.close.called
