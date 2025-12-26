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

import psycopg
import pytest
from coreason_etl_euctr.postgres_loader import PostgresLoader


@pytest.fixture  # type: ignore[misc]
def mock_psycopg_connect() -> Generator[MagicMock, None, None]:
    with patch("psycopg.connect") as mock:
        yield mock


def test_connect_success(mock_psycopg_connect: MagicMock) -> None:
    """Test successful connection."""
    loader = PostgresLoader()
    loader.connect()
    mock_psycopg_connect.assert_called_once()
    assert loader.conn is not None


def test_connect_failure(mock_psycopg_connect: MagicMock) -> None:
    """Test connection failure raises exception."""
    mock_psycopg_connect.side_effect = psycopg.Error("Connection refused")
    loader = PostgresLoader()
    with pytest.raises(psycopg.Error):
        loader.connect()


def test_close_connection(mock_psycopg_connect: MagicMock) -> None:
    """Test closing the connection."""
    mock_conn = MagicMock()
    mock_psycopg_connect.return_value = mock_conn
    loader = PostgresLoader()
    loader.connect()
    loader.close()
    mock_conn.close.assert_called_once()
    assert loader.conn is None


def test_prepare_schema_success(mock_psycopg_connect: MagicMock) -> None:
    """Test schema preparation executes correct SQL."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()
    loader.prepare_schema()

    # Verify 3 create table statements were executed
    assert mock_cursor.execute.call_count == 3
    # Check if commit was called
    mock_conn.commit.assert_called_once()


def test_prepare_schema_failure(mock_psycopg_connect: MagicMock) -> None:
    """Test schema preparation rolls back on error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg.Error("Syntax error")
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()
    with pytest.raises(psycopg.Error):
        loader.prepare_schema()

    mock_conn.rollback.assert_called_once()


def test_prepare_schema_not_connected() -> None:
    """Test prepare_schema raises RuntimeError if not connected."""
    loader = PostgresLoader()
    with pytest.raises(RuntimeError, match="Database not connected"):
        loader.prepare_schema()


def test_bulk_load_stream_success(mock_psycopg_connect: MagicMock) -> None:
    """Test bulk load uses copy."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()

    data = io.StringIO("col1,col2\nval1,val2")
    loader.bulk_load_stream(data, "test_table")

    # Verify copy called with correct SQL
    mock_cursor.copy.assert_called_once()
    args, _ = mock_cursor.copy.call_args
    assert "COPY test_table FROM STDIN" in args[0]
    # Verify data written
    assert mock_copy.write.called


def test_bulk_load_stream_failure(mock_psycopg_connect: MagicMock) -> None:
    """Test bulk load raises error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.side_effect = psycopg.Error("Copy failed")
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()

    data = io.StringIO("data")
    with pytest.raises(psycopg.Error):
        loader.bulk_load_stream(data, "test_table")


def test_bulk_load_not_connected() -> None:
    """Test bulk_load raises RuntimeError if not connected."""
    loader = PostgresLoader()
    data = io.StringIO("data")
    with pytest.raises(RuntimeError):
        loader.bulk_load_stream(data, "test_table")


def test_commit_rollback_safe(mock_psycopg_connect: MagicMock) -> None:
    """Test commit/rollback are safe even if not connected (or handle None gracefully)."""
    loader = PostgresLoader()
    # Should not raise error if conn is None
    loader.commit()
    loader.rollback()

    mock_conn = MagicMock()
    mock_psycopg_connect.return_value = mock_conn
    loader.connect()

    loader.commit()
    mock_conn.commit.assert_called_once()

    loader.rollback()
    mock_conn.rollback.assert_called_once()


def test_upsert_stream_success(mock_psycopg_connect: MagicMock) -> None:
    """Test upsert stream executes correct sequence of SQL."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    mock_psycopg_connect.return_value = mock_conn

    # Mock description to return columns
    Column = MagicMock()
    Column.name = "id"
    Column2 = MagicMock()
    Column2.name = "val"
    mock_cursor.description = [Column, Column2]

    loader = PostgresLoader()
    loader.connect()

    data = io.StringIO("id,val\n1,a")
    loader.upsert_stream(data, "test_table", conflict_keys=["id"])

    # Verify calls
    # 1. Create Temp Table
    # 2. Copy
    # 3. Select (to get cols) - actually we check description after this call
    # 4. Insert ... On Conflict ... Update
    # 5. Drop Temp

    calls = mock_cursor.execute.call_args_list
    assert len(calls) >= 4  # Create, Select, Insert, Drop

    # Check Create
    assert "CREATE TEMP TABLE" in calls[0][0][0]
    assert "test_table_staging_" in calls[0][0][0]

    # Check Copy
    mock_cursor.copy.assert_called_once()

    # Check Select/Description access (implicit in code flow)

    # Check Insert
    # Find the call with INSERT
    insert_call = next((call for call in calls if "INSERT INTO test_table" in call[0][0]), None)
    assert insert_call is not None
    sql = insert_call[0][0]
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "val = EXCLUDED.val" in sql
    assert "id = EXCLUDED.id" not in sql  # PK should not be in SET clause usually

    # Check Drop
    assert "DROP TABLE IF EXISTS" in calls[-1][0][0]


def test_upsert_stream_no_columns(mock_psycopg_connect: MagicMock) -> None:
    """Test upsert aborts if no columns found in temp table."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value = mock_conn

    # Return None or empty description
    mock_cursor.description = []

    loader = PostgresLoader()
    loader.connect()
    data = io.StringIO("id,val\n1,a")

    loader.upsert_stream(data, "test_table", conflict_keys=["id"])

    # Should create temp, copy, then stop before insert
    insert_calls = [call for call in mock_cursor.execute.call_args_list if "INSERT INTO" in call[0][0]]
    assert len(insert_calls) == 0


def test_upsert_stream_missing_conflict_keys() -> None:
    """Test ValueError if conflict_keys missing."""
    loader = PostgresLoader()
    loader.conn = MagicMock()  # fake connection
    data = io.StringIO("data")

    with pytest.raises(ValueError, match="Conflict keys required"):
        loader.upsert_stream(data, "table", conflict_keys=[])


def test_upsert_stream_not_connected() -> None:
    """Test RuntimeError if not connected."""
    loader = PostgresLoader()
    data = io.StringIO("data")
    with pytest.raises(RuntimeError):
        loader.upsert_stream(data, "table", conflict_keys=["id"])


def test_upsert_stream_only_pks(mock_psycopg_connect: MagicMock) -> None:
    """Test upsert when only PKs exist (DO NOTHING)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg_connect.return_value = mock_conn

    # Mock description to return only PK column
    Column = MagicMock()
    Column.name = "id"
    mock_cursor.description = [Column]

    loader = PostgresLoader()
    loader.connect()

    data = io.StringIO("id\n1")
    loader.upsert_stream(data, "test_table", conflict_keys=["id"])

    calls = mock_cursor.execute.call_args_list
    # Check Insert
    insert_call = next((call for call in calls if "INSERT INTO test_table" in call[0][0]), None)
    assert insert_call is not None
    sql = insert_call[0][0]
    assert "ON CONFLICT (id) DO NOTHING" in sql


def test_upsert_stream_failure(mock_psycopg_connect: MagicMock) -> None:
    """Test upsert stream failure (exception handling)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    # Fail on create temp table
    mock_cursor.execute.side_effect = psycopg.Error("Upsert error")
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()
    data = io.StringIO("id,val\n1,a")

    with pytest.raises(psycopg.Error, match="Upsert error"):
        loader.upsert_stream(data, "test_table", conflict_keys=["id"])
