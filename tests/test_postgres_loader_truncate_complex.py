# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock

import psycopg
import pytest

from coreason_etl_euctr.postgres_loader import PostgresLoader


def test_truncate_large_list_of_tables() -> None:
    """Test truncating a large number of tables."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    # Generate 100 table names
    tables = [f"table_{i}" for i in range(100)]
    loader.truncate_tables(tables)

    expected_sql = f"TRUNCATE TABLE {', '.join(tables)} CASCADE"
    mock_cursor.execute.assert_called_once_with(expected_sql)


def test_truncate_tables_with_schema_qualification() -> None:
    """Test truncating tables with schema qualifiers (e.g. public.eu_trials)."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    tables = ["public.eu_trials", "staging.raw_data"]
    loader.truncate_tables(tables)

    expected_sql = "TRUNCATE TABLE public.eu_trials, staging.raw_data CASCADE"
    mock_cursor.execute.assert_called_once_with(expected_sql)


def test_truncate_tables_whitespaces() -> None:
    """
    Test that whitespace in inputs is preserved or handled.
    The current implementation does a simple join.
    If input is messy, the SQL will be messy.
    This test documents current behavior (GIGO: Garbage In, Garbage Out).
    """
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    tables = ["  table1 ", "table2"]
    loader.truncate_tables(tables)

    # Current implementation: "TRUNCATE TABLE   table1 , table2 CASCADE"
    # Postgres might choke on this if not quoted, or it might strip it.
    # We assert the string construction is exact.
    expected_sql = "TRUNCATE TABLE   table1 , table2 CASCADE"
    mock_cursor.execute.assert_called_once_with(expected_sql)


def test_truncate_chained_execution() -> None:
    """Test sequential calls to verify cursor handling."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Ensure a fresh cursor is returned each time
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    loader.truncate_tables(["t1"])
    loader.truncate_tables(["t2"])

    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE t1 CASCADE")
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE t2 CASCADE")


def test_truncate_connection_closed_mid_operation() -> None:
    """Test behavior if connection is closed/None before execution."""
    loader = PostgresLoader()
    # Initially connected
    loader.conn = MagicMock()

    # But then set to None (simulating a close or loss)
    loader.conn = None

    with pytest.raises(RuntimeError, match="Database not connected"):
        loader.truncate_tables(["t1"])


def test_truncate_psycopg_operational_error() -> None:
    """Test specific database operational errors."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    # Simulate a deadlock or lock timeout
    mock_cursor.execute.side_effect = psycopg.OperationalError("deadlock detected")

    with pytest.raises(psycopg.Error, match="deadlock detected"):
        loader.truncate_tables(["t1"])
