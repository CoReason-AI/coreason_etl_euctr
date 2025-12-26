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


@pytest.fixture
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
