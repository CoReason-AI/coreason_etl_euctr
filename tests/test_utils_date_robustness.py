# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date

import pytest
from coreason_etl_euctr.utils import parse_flexible_date


def test_parse_flexible_date_extended() -> None:
    """Test extended date formats for robustness."""
    # Already supported
    assert parse_flexible_date("2023-01-31") == date(2023, 1, 31)
    assert parse_flexible_date("31/01/2023") == date(2023, 1, 31)
    assert parse_flexible_date("31.01.2023") == date(2023, 1, 31)

    # New formats to support
    assert parse_flexible_date("31-01-2023") == date(2023, 1, 31)
    assert parse_flexible_date("2023/01/31") == date(2023, 1, 31)

    # Textual months (English)
    assert parse_flexible_date("31 Jan 2023") == date(2023, 1, 31)
    assert parse_flexible_date("31 January 2023") == date(2023, 1, 31)
    assert parse_flexible_date("Jan 31, 2023") == date(2023, 1, 31)
    assert parse_flexible_date("January 31, 2023") == date(2023, 1, 31)


def test_parse_flexible_date_invalid() -> None:
    """Test invalid dates."""
    with pytest.raises(ValueError):
        parse_flexible_date("2023-02-30")  # Invalid day
    with pytest.raises(ValueError):
        parse_flexible_date("Not a date")

    # Ambiguous? 01-02-2023. Usually EU is DD-MM.
    # We assume DD-MM if not ISO.
    assert parse_flexible_date("01-02-2023") == date(2023, 2, 1)
