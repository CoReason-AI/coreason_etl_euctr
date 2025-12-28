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
from coreason_etl_euctr.postgres_loader import PostgresLoader


@pytest.fixture  # type: ignore[misc]
def mock_psycopg_connect() -> Generator[MagicMock, None, None]:
    with patch("psycopg.connect") as mock:
        yield mock


def test_bulk_load_dynamic_columns(mock_psycopg_connect: MagicMock) -> None:
    """
    Test that bulk_load_stream reads the header and constructs a COPY command
    with explicit columns, handling schema mismatch (e.g. auto-increment ID in DB).
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()

    # CSV with 'col_a,col_b', DB might have 'id,col_a,col_b'
    # The loader should extract 'col_a,col_b' and use COPY table (col_a,col_b)...
    # Note: The first read returns the header line (simulating Pipeline behavior)
    data = io.StringIO("col_a,col_b\nval1,val2")

    loader.bulk_load_stream(data, "test_table")

    mock_cursor.copy.assert_called_once()
    sql = mock_cursor.copy.call_args[0][0]

    # Expectation: Explicit columns, NO HEADER (since we consumed it)
    # Note: implementation uses ", ".join so there are spaces
    assert 'COPY test_table ("col_a", "col_b") FROM STDIN' in sql
    assert "HEADER" not in sql

    # Verify data written (should be just values)
    # The first write might vary depending on implementation details,
    # but we expect 'val1,val2' to be written eventually.
    # Since StringIO read might return rest, check if write was called.
    assert mock_copy.write.call_count >= 1


def test_upsert_stream_dynamic_columns(mock_psycopg_connect: MagicMock) -> None:
    """
    Test that upsert_stream reads the header and constructs a COPY command
    with explicit columns for the temp table.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    # Mock description for the final INSERT phase
    Col = MagicMock()
    Col.name = "col_a"
    mock_cursor.description = [Col]

    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()

    data = io.StringIO("col_a\nval1")

    loader.upsert_stream(data, "test_table", conflict_keys=["col_a"])

    # Check the COPY command for the temp table
    # It calls copy() once.
    mock_cursor.copy.assert_called_once()
    sql = mock_cursor.copy.call_args[0][0]

    # Extract temp table name from SQL to verify COPY uses it
    # CREATE TEMP TABLE {temp} ...
    # COPY {temp} (col_a) ...
    assert "COPY" in sql
    assert '("col_a") FROM STDIN' in sql
    assert "HEADER" not in sql
