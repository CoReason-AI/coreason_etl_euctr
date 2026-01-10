# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date, datetime
from typing import Optional

from coreason_etl_euctr.pipeline import Pipeline
from pydantic import BaseModel


class ComplexModel(BaseModel):
    id: int
    text_field: str
    date_field: date
    datetime_field: datetime
    bool_field: bool
    nullable_field: Optional[str] = None


def test_csv_injection_and_escaping() -> None:
    """
    Test that fields containing CSV delimiters (commas, quotes, newlines)
    are correctly escaped.
    """
    pipeline = Pipeline()

    # Case 1: Comma in text
    # Case 2: Quotes in text
    # Case 3: Newline in text
    data = [
        ComplexModel(
            id=1,
            text_field="Hello, World",
            date_field=date(2023, 1, 1),
            datetime_field=datetime(2023, 1, 1, 12, 0, 0),
            bool_field=True,
        ),
        ComplexModel(
            id=2,
            text_field='She said "Hi"',
            date_field=date(2023, 1, 1),
            datetime_field=datetime(2023, 1, 1, 12, 0, 0),
            bool_field=False,
        ),
        ComplexModel(
            id=3,
            text_field="Line 1\nLine 2",
            date_field=date(2023, 1, 1),
            datetime_field=datetime(2023, 1, 1, 12, 0, 0),
            bool_field=True,
        ),
    ]

    chunks = list(pipeline.stage_data(data))
    full_csv = "".join(chunks)

    # CSV rules:
    # 1. 'Hello, World' -> "Hello, World"
    # 2. 'She said "Hi"' -> "She said ""Hi"""
    # 3. 'Line 1\nLine 2' -> "Line 1\nLine 2" (usually quoted)

    # Assertions
    assert '"Hello, World"' in full_csv
    assert '"She said ""Hi"""' in full_csv
    assert '"Line 1\nLine 2"' in full_csv


def test_unicode_handling() -> None:
    """Test handling of multi-byte Unicode characters (emojis, diverse scripts)."""
    pipeline = Pipeline()
    text = "Ω ≈ ç √ ∫ ˜ µ ≤ ≥ ÷ ☀ ☁ ☂ ☃ ☄ ★ ☆ ⚡ 埴 塙 稟 💂 🎅  Christmas"

    data = [
        ComplexModel(
            id=1,
            text_field=text,
            date_field=date(2023, 1, 1),
            datetime_field=datetime(2023, 1, 1, 12, 0, 0),
            bool_field=True,
        )
    ]

    chunks = list(pipeline.stage_data(data))
    full_csv = "".join(chunks)

    assert text in full_csv


def test_serialization_formats() -> None:
    """
    Verify that Pydantic types are serialized to string formats compatible with Postgres.
    - Dates: YYYY-MM-DD
    - Datetimes: ISO format
    - Booleans: 'True'/'False' (Postgres accepts these)
    """
    pipeline = Pipeline()
    dt = datetime(2023, 10, 25, 14, 30, 0)
    d = date(2023, 10, 25)

    data = [ComplexModel(id=1, text_field="test", date_field=d, datetime_field=dt, bool_field=True)]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1]

    # Expected: 1,test,2023-10-25,2023-10-25T14:30:00,True,
    assert "2023-10-25" in row
    # ISO format might include T or space depending on Pydantic version defaults.
    # Pydantic v2 usually outputs ISO 8601 with 'T'.
    assert "2023-10-25T14:30:00" in row
    assert "True" in row


def test_large_dataset_performance() -> None:
    """
    Verify that the pipeline can handle a larger stream without error.
    This is a lightweight stress test.
    """
    pipeline = Pipeline()
    count = 1000

    # Generator expression
    data = (
        ComplexModel(
            id=i,
            text_field=f"Row {i}",
            date_field=date(2023, 1, 1),
            datetime_field=datetime(2023, 1, 1, 12, 0, 0),
            bool_field=(i % 2 == 0),
        )
        for i in range(count)
    )

    chunks = list(pipeline.stage_data(data))

    # Header + 1000 rows = 1001 chunks (since we yield header, then row 1, then row...)
    # Actually implementation yields:
    # 1. Header
    # 2. Row 1
    # 3. Row 2 ...
    assert len(chunks) == count + 1

    # Verify last row
    assert f"Row {count - 1}" in chunks[-1]
