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
import logging
from typing import Generator
from unittest.mock import MagicMock

import pytest
from coreason_etl_euctr.postgres_loader import PostgresLoader
from loguru import logger as loguru_logger


class PropagateHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        logging.getLogger(record.name).handle(record)


@pytest.fixture  # type: ignore[misc]
def caplog_loguru(caplog: pytest.LogCaptureFixture) -> Generator[pytest.LogCaptureFixture, None, None]:
    """
    Fixture to allow caplog to capture loguru logs.
    """
    handler_id = loguru_logger.add(PropagateHandler(), format="{message}")
    yield caplog
    loguru_logger.remove(handler_id)


def test_bulk_load_zero_rows(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """Test logging when loading 0 rows (header only)."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = MagicMock()
    mock_cursor.rowcount = 0  # 0 rows affected

    loader.conn = mock_conn

    # Header only
    data = io.StringIO("col1,col2\n")
    loader.bulk_load_stream(data, "test_table")

    assert "Bulk loaded 0 rows into test_table" in caplog_loguru.text


def test_upsert_do_nothing_action(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """
    Test upsert logging when action is DO NOTHING (e.g. only PK columns).
    If we have 3 input rows, 1 conflicts (ignored), 2 new (inserted).
    Row count should be 2.
    """
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = MagicMock()

    # Upsert logic queries for columns.
    # If we pretend table only has PK 'id', update assignments will be empty.
    mock_desc = MagicMock()
    mock_desc.name = "id"
    mock_cursor.description = [mock_desc]

    # Simulate result: 2 rows inserted
    mock_cursor.rowcount = 2

    loader.conn = mock_conn

    data = io.StringIO("id\n1\n2\n3")
    loader.upsert_stream(data, "test_pk_only", conflict_keys=["id"])

    # Verify the SQL constructed uses DO NOTHING?
    # We can check call args if we want, but main goal here is logging.
    calls = mock_cursor.execute.call_args_list
    # The last execute before drop should be the INSERT
    insert_call = calls[-2] # Last is DROP, second last is INSERT
    sql = insert_call[0][0]
    assert "DO NOTHING" in sql

    assert "Upserted 2 rows into test_pk_only" in caplog_loguru.text


def test_upsert_mixed_insert_update(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """
    Test upsert logging with mixed inserts and updates.
    Postgres counts both.
    """
    loader = PostgresLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = MagicMock()

    # Table has id, val. val is updatable.
    mock_desc_id = MagicMock()
    mock_desc_id.name = "id"
    mock_desc_val = MagicMock()
    mock_desc_val.name = "val"
    mock_cursor.description = [mock_desc_id, mock_desc_val]

    # Simulate result: 5 rows processed (3 inserts, 2 updates) -> rowcount 5
    mock_cursor.rowcount = 5

    loader.conn = mock_conn

    data = io.StringIO("id,val\n1,a\n2,b")
    loader.upsert_stream(data, "test_mixed", conflict_keys=["id"])

    # Verify SQL uses UPDATE SET
    calls = mock_cursor.execute.call_args_list
    insert_call = calls[-2]
    sql = insert_call[0][0]
    assert "DO UPDATE SET" in sql

    assert "Upserted 5 rows into test_mixed" in caplog_loguru.text


def test_bulk_load_empty_stream_logging(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """
    Test logging (or lack thereof) when stream is completely empty (no header).
    Existing code warns and skips.
    """
    loader = PostgresLoader()
    mock_conn = MagicMock()
    loader.conn = mock_conn

    data = io.StringIO("")
    loader.bulk_load_stream(data, "test_table")

    assert "Empty stream for test_table" in caplog_loguru.text
    # Should NOT log success message
    assert "Bulk loaded" not in caplog_loguru.text
