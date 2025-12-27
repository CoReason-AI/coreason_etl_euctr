# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock, call

import psycopg
import pytest

from coreason_etl_euctr.postgres_loader import PostgresLoader


def test_truncate_tables_success() -> None:
    """Test successful truncation of tables."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    tables = ["eu_trials", "eu_trial_drugs"]
    loader.truncate_tables(tables)

    # Verify SQL execution
    expected_sql = "TRUNCATE TABLE eu_trials, eu_trial_drugs CASCADE"
    mock_cursor.execute.assert_called_once_with(expected_sql)


def test_truncate_tables_no_connection() -> None:
    """Test truncate raises error if not connected."""
    loader = PostgresLoader()
    loader.conn = None

    with pytest.raises(RuntimeError, match="Database not connected"):
        loader.truncate_tables(["table1"])


def test_truncate_tables_empty_list() -> None:
    """Test truncate does nothing if list is empty."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    loader.conn = mock_conn

    loader.truncate_tables([])

    # Should not create cursor or execute anything
    mock_conn.cursor.assert_not_called()


def test_truncate_tables_error() -> None:
    """Test error handling during truncation."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.conn = mock_conn

    # Simulate DB error
    mock_cursor.execute.side_effect = psycopg.Error("Truncate failed")

    with pytest.raises(psycopg.Error, match="Truncate failed"):
        loader.truncate_tables(["table1"])
