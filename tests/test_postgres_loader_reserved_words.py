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
from typing import IO, cast
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.postgres_loader import PostgresLoader


class MockStringIteratorIO(io.TextIOBase):
    """Simple mock stream that yields chunks."""

    def __init__(self, chunks: list[str]):
        self.chunks = iter(chunks)

    def read(self, size: int | None = -1) -> str:
        try:
            return next(self.chunks)
        except StopIteration:
            return ""


@pytest.fixture  # type: ignore[misc]
def mock_psycopg_connect():
    with patch("psycopg.connect") as mock:
        yield mock


def test_bulk_load_quotes_reserved_words(mock_psycopg_connect: MagicMock) -> None:
    """
    Test that bulk_load_stream correctly quotes column names that are reserved keywords
    or contain special characters, preventing SQL syntax errors.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    mock_psycopg_connect.return_value = mock_conn

    loader = PostgresLoader()
    loader.connect()

    # Input has 'desc' (reserved) and 'my-col' (hyphen requires quotes)
    # Header: id,desc,my-col
    data = MockStringIteratorIO(["id,desc,my-col\n", "1,test,val"])

    loader.bulk_load_stream(cast(IO[str], data), "test_table")

    mock_cursor.copy.assert_called_once()
    args, _ = mock_cursor.copy.call_args
    sql = args[0]

    # We expect identifiers to be quoted: "id", "desc", "my-col"
    assert '("id", "desc", "my-col")' in sql or '("id", "desc", "my-col")' in sql.replace(", ", ",")


def test_upsert_quotes_reserved_words(mock_psycopg_connect: MagicMock) -> None:
    """
    Test that upsert_stream correctly quotes column names in generated SQL.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_copy = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy
    mock_psycopg_connect.return_value = mock_conn

    # Mock description
    Col1 = MagicMock()
    Col1.name = "id"
    Col2 = MagicMock()
    Col2.name = "desc" # Reserved
    mock_cursor.description = [Col1, Col2]

    loader = PostgresLoader()
    loader.connect()

    data = MockStringIteratorIO(["id,desc\n", "1,text"])
    loader.upsert_stream(cast(IO[str], data), "test_table", conflict_keys=["id"])

    # 1. Check COPY to temp table
    mock_cursor.copy.assert_called_once()
    copy_sql = mock_cursor.copy.call_args[0][0]
    assert '("id", "desc")' in copy_sql or '("id", "desc")' in copy_sql.replace(", ", ",")

    # 2. Check INSERT ... ON CONFLICT
    # We need to find the execute call for INSERT
    insert_calls = [c[0][0] for c in mock_cursor.execute.call_args_list if "INSERT INTO" in c[0][0]]
    assert len(insert_calls) > 0
    insert_sql = insert_calls[0]

    # Check quotes in INSERT keys
    assert 'INSERT INTO test_table ("id", "desc")' in insert_sql

    # Check quotes in SELECT
    assert 'SELECT "id", "desc" FROM' in insert_sql

    # Check quotes in UPDATE SET
    # desc = EXCLUDED.desc -> "desc" = EXCLUDED."desc"
    assert '"desc" = EXCLUDED."desc"' in insert_sql
