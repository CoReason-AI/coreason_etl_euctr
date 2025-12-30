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
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from coreason_etl_euctr.postgres_loader import PostgresLoader


@pytest.fixture  # type: ignore[misc]
def mock_psycopg_connect() -> MagicMock:
    with patch("psycopg.connect") as mock:
        yield mock


@pytest.fixture  # type: ignore[misc]
def loader(mock_psycopg_connect: MagicMock) -> PostgresLoader:
    loader = PostgresLoader()
    # Manually trigger connect so self.conn is set
    loader.connect()
    return loader


def test_connect_success(mock_psycopg_connect: MagicMock) -> None:
    loader = PostgresLoader()
    loader.connect()
    mock_psycopg_connect.assert_called_once()
    assert loader.conn is not None


def test_connect_fail(mock_psycopg_connect: MagicMock) -> None:
    mock_psycopg_connect.side_effect = psycopg.Error("Connection Failed")
    loader = PostgresLoader()
    with pytest.raises(psycopg.Error):
        loader.connect()


def test_close(loader: PostgresLoader) -> None:
    conn = loader.conn
    loader.close()
    conn.close.assert_called_once()  # type: ignore
    assert loader.conn is None


def test_prepare_schema_success(loader: PostgresLoader) -> None:
    loader.prepare_schema()
    # Verify cursor usage
    loader.conn.cursor.assert_called()  # type: ignore
    # Commit called
    loader.conn.commit.assert_called()  # type: ignore


def test_prepare_schema_not_connected() -> None:
    l = PostgresLoader()
    with pytest.raises(RuntimeError):
        l.prepare_schema()


def test_prepare_schema_fail(loader: PostgresLoader) -> None:
    # Cursor context manager raises error
    cursor_mock = MagicMock()
    cursor_mock.execute.side_effect = psycopg.Error("DDL Fail")
    loader.conn.cursor.return_value.__enter__.return_value = cursor_mock  # type: ignore

    with pytest.raises(psycopg.Error):
        loader.prepare_schema()

    loader.conn.rollback.assert_called()  # type: ignore


def test_bulk_load_stream(loader: PostgresLoader) -> None:
    stream = io.StringIO("col1,col2\nval1,val2\n")

    cursor_mock = MagicMock()
    copy_mock = MagicMock()
    cursor_mock.copy.return_value.__enter__.return_value = copy_mock
    loader.conn.cursor.return_value.__enter__.return_value = cursor_mock  # type: ignore

    loader.bulk_load_stream(stream, "my_table")

    # Verify SQL
    call_args = cursor_mock.copy.call_args[0][0]
    assert "COPY my_table" in call_args
    assert "(\"col1\", \"col2\")" in call_args

    # Verify write
    # The header is read and stripped.
    # "val1,val2\n" remains.
    # copy.write called with remaining chunk
    copy_mock.write.assert_called()


def test_bulk_load_empty_stream(loader: PostgresLoader) -> None:
    stream = io.StringIO("")
    loader.bulk_load_stream(stream, "t")
    # Should not call copy
    loader.conn.cursor.assert_not_called()  # type: ignore


def test_bulk_load_fail(loader: PostgresLoader) -> None:
    stream = io.StringIO("h\nd")
    loader.conn.cursor.side_effect = psycopg.Error("Copy Fail")  # type: ignore
    with pytest.raises(psycopg.Error):
        loader.bulk_load_stream(stream, "t")

def test_bulk_load_empty_header_read(loader: PostgresLoader) -> None:
    """Test when stream read() returns empty string immediately."""
    # This hits line 117 (if not header_chunk)
    stream = MagicMock()
    stream.read.return_value = ""
    loader.bulk_load_stream(stream, "t")
    loader.conn.cursor.assert_not_called()


def test_upsert_stream(loader: PostgresLoader) -> None:
    stream = io.StringIO("id,val\n1,a\n")

    cursor_mock = MagicMock()
    copy_mock = MagicMock()
    cursor_mock.copy.return_value.__enter__.return_value = copy_mock

    # Mock description for collecting columns
    col_desc = [MagicMock(name="id"), MagicMock(name="val")]
    col_desc[0].name = "id"
    col_desc[1].name = "val"
    cursor_mock.description = col_desc

    loader.conn.cursor.return_value.__enter__.return_value = cursor_mock  # type: ignore

    loader.upsert_stream(stream, "t", conflict_keys=["id"])

    # 1. Create Temp
    cursor_mock.execute.assert_any_call(pytest_any_string_sql_like("CREATE TEMP TABLE"))

    # 2. Copy
    cursor_mock.copy.assert_called()

    # 3. Insert On Conflict
    # We check if execute was called with INSERT and ON CONFLICT
    # Since we can't easily match exact string due to random suffix in temp table,
    # we verify general structure if possible, or just that it was called.

    # We can inspect calls
    calls = cursor_mock.execute.call_args_list
    # Expected calls: CREATE TEMP, SELECT * LIMIT 0 (to get schema), INSERT ..., DROP TEMP (maybe)

    # Check for INSERT
    insert_calls = [c for c in calls if "INSERT INTO t" in c[0][0]]
    assert len(insert_calls) == 1
    sql = insert_calls[0][0][0]
    assert "ON CONFLICT (\"id\") DO UPDATE SET \"val\" = EXCLUDED.\"val\"" in sql


def test_upsert_no_conflict_keys(loader: PostgresLoader) -> None:
    with pytest.raises(ValueError):
        loader.upsert_stream(io.StringIO(), "t", [])


def test_upsert_empty(loader: PostgresLoader) -> None:
    loader.upsert_stream(io.StringIO(""), "t", ["id"])
    loader.conn.cursor.assert_not_called()  # type: ignore

def test_upsert_empty_read(loader: PostgresLoader) -> None:
    """Test when upsert stream read returns empty."""
    # Hits lines 165/267 check
    stream = MagicMock()
    stream.read.return_value = ""
    loader.upsert_stream(stream, "t", ["id"])
    loader.conn.cursor.assert_not_called()


def test_truncate_tables(loader: PostgresLoader) -> None:
    loader.truncate_tables(["t1", "t2"])
    loader.conn.cursor.return_value.__enter__.return_value.execute.assert_called_with("TRUNCATE TABLE t1, t2 CASCADE") # type: ignore


def test_truncate_fail(loader: PostgresLoader) -> None:
    loader.conn.cursor.side_effect = psycopg.Error("Truncate Fail") # type: ignore
    with pytest.raises(psycopg.Error):
        loader.truncate_tables(["t"])

def test_truncate_empty_list(loader: PostgresLoader) -> None:
    loader.truncate_tables([])
    # Should do nothing
    loader.conn.cursor.assert_not_called()

def test_upsert_empty_description(loader: PostgresLoader) -> None:
    # Test case where temp table has no columns (weird but possible edge case)
    stream = io.StringIO("id\n1\n")
    cursor_mock = MagicMock()
    cursor_mock.description = None # Simulate no columns found

    # Needs to handle COPY first
    copy_mock = MagicMock()
    cursor_mock.copy.return_value.__enter__.return_value = copy_mock
    loader.conn.cursor.return_value.__enter__.return_value = cursor_mock

    loader.upsert_stream(stream, "t", ["id"])

    # Should return early after bulk loading temp table, before UPSERT SQL generation
    # Verify execute NOT called with INSERT
    execute_calls = [str(c) for c in cursor_mock.execute.call_args_list]
    assert not any("INSERT INTO" in c for c in execute_calls)

def test_upsert_no_updates(loader: PostgresLoader) -> None:
    # Test case where update_assignments is empty (only conflict keys in table)
    # Conflict key "id", column "id". update set empty.
    stream = io.StringIO("id\n1\n")
    cursor_mock = MagicMock()

    col = MagicMock()
    col.name = "id"
    cursor_mock.description = [col]

    copy_mock = MagicMock()
    cursor_mock.copy.return_value.__enter__.return_value = copy_mock
    loader.conn.cursor.return_value.__enter__.return_value = cursor_mock

    loader.upsert_stream(stream, "t", ["id"])

    # Should generate ON CONFLICT DO NOTHING
    insert_calls = [c[0][0] for c in cursor_mock.execute.call_args_list if "INSERT INTO" in str(c)]
    assert len(insert_calls) == 1
    assert "DO NOTHING" in insert_calls[0]


def test_transaction_methods(loader: PostgresLoader) -> None:
    loader.commit()
    loader.conn.commit.assert_called() # type: ignore
    loader.rollback()
    loader.conn.rollback.assert_called() # type: ignore


class AnyStringSqlLike:
    def __init__(self, substr):
        self.substr = substr
    def __eq__(self, other):
        return isinstance(other, str) and self.substr in other

def pytest_any_string_sql_like(substr):
    return AnyStringSqlLike(substr)
