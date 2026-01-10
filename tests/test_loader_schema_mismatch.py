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
from unittest.mock import MagicMock

import pytest

from coreason_etl_euctr.postgres_loader import PostgresLoader


def test_loader_schema_mismatch_extra_columns() -> None:
    """
    Test behavior when CSV has columns not in DB schema.
    PostgresLoader reads the header row to construct COPY command.
    If the CSV header contains "extra_col" and we do `COPY table (col1, extra_col)`,
    Postgres will error if `extra_col` does not exist in table.
    We mock the psycopg error to verify it's caught and raised.
    """
    loader = PostgresLoader()
    # Mypy dislikes direct method assignment. We just set conn.
    loader.conn = MagicMock()

    # Mock cursor and copy
    mock_cursor = MagicMock()
    loader.conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate Postgres error on COPY
    # psycopg.Error is what we catch.
    import psycopg

    mock_cursor.copy.side_effect = psycopg.Error("column 'extra_col' of relation 'eu_trials' does not exist")

    csv_data = "eudract_number,extra_col\n123,val"
    stream = io.StringIO(csv_data)

    with pytest.raises(psycopg.Error, match="column 'extra_col'"):
        loader.bulk_load_stream(stream, "eu_trials")


def test_loader_empty_stream_no_header() -> None:
    """Test handling of completely empty stream (0 bytes)."""
    loader = PostgresLoader()
    loader.conn = MagicMock()

    stream = io.StringIO("")
    # Should log warning and return, no error.
    loader.bulk_load_stream(stream, "eu_trials")

    loader.conn.cursor.assert_not_called()
