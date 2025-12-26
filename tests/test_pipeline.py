# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import List

import pytest
from pydantic import BaseModel

from coreason_etl_euctr.pipeline import Pipeline


class MockModel(BaseModel):
    id: int
    name: str
    active: bool = True


def test_stage_data_empty() -> None:
    """Test that an empty iterator yields nothing."""
    pipeline = Pipeline()
    result = list(pipeline.stage_data([]))
    assert result == []


def test_stage_data_basic() -> None:
    """Test basic CSV generation with headers."""
    pipeline = Pipeline()
    data = [MockModel(id=1, name="Test")]

    # Use list() to consume the generator
    chunks = list(pipeline.stage_data(data))

    # Expect header chunk and data chunk
    assert len(chunks) == 2

    # Check header (standard CSV format uses CRLF usually, but csv module might use \r\n or \n depending on system)
    # csv.writer default is \r\n
    assert "id,name,active" in chunks[0]

    # Check data
    assert "1,Test,True" in chunks[1]


def test_stage_data_deduplication() -> None:
    """Test that identical rows are filtered out."""
    pipeline = Pipeline()
    data = [
        MockModel(id=1, name="A"),
        MockModel(id=1, name="A"),  # Duplicate
        MockModel(id=2, name="B"),
    ]

    chunks = list(pipeline.stage_data(data))
    full_csv = "".join(chunks)
    lines = full_csv.strip().splitlines()

    # Lines: Header, Row 1, Row 2 (Duplicate skipped)
    # Total lines including header should be 3
    assert len(lines) == 3
    assert "1,A,True" in full_csv
    assert "2,B,True" in full_csv

    # Count occurrences of "1,A,True"
    assert full_csv.count("1,A,True") == 1


def test_stage_data_multiple_chunks() -> None:
    """Test that generator yields chunks progressively."""
    pipeline = Pipeline()
    data = (MockModel(id=i, name=f"Name{i}") for i in range(5))

    gen = pipeline.stage_data(data)

    # First yield is header
    header = next(gen)
    assert "id,name,active" in header

    # Next yields are rows
    row1 = next(gen)
    assert "0,Name0,True" in row1

    row2 = next(gen)
    assert "1,Name1,True" in row2


def test_stage_data_handles_none_values() -> None:
    """Test handling of None/Empty fields."""
    class NullableModel(BaseModel):
        f1: str | None
        f2: int

    pipeline = Pipeline()
    data = [NullableModel(f1=None, f2=10)]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1].strip()

    # Expect empty string for None: ,10
    assert ",10" in row
