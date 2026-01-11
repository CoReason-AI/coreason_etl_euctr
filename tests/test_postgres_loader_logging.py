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


def test_bulk_load_logging_row_count(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """Test that bulk_load_stream logs the row count."""
    loader = PostgresLoader()

    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Configure mock
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = MagicMock()

    # Simulate rowcount
    mock_cursor.rowcount = 42

    loader.conn = mock_conn

    data = io.StringIO("col1,col2\n1,2\n3,4")
    loader.bulk_load_stream(data, "test_table")

    # Check log message
    assert "Bulk loaded 42 rows into test_table" in caplog_loguru.text


def test_upsert_logging_row_count(caplog_loguru: pytest.LogCaptureFixture) -> None:
    """Test that upsert_stream logs the row count."""
    loader = PostgresLoader()

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = MagicMock()

    # Simulate rowcount for INSERT ... SELECT
    mock_cursor.rowcount = 15
    # Also need description for columns
    mock_desc = MagicMock()
    mock_desc.name = "col1"
    mock_cursor.description = [mock_desc]

    loader.conn = mock_conn

    data = io.StringIO("col1\n1")
    loader.upsert_stream(data, "test_table", conflict_keys=["col1"])

    assert "Upserted 15 rows into test_table" in caplog_loguru.text
