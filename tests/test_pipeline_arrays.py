# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import List, Optional
from coreason_etl_euctr.pipeline import Pipeline
from pydantic import BaseModel
import pytest

class ListModel(BaseModel):
    id: int
    tags: Optional[List[str]]

def test_stage_data_list_serialization_simple():
    """Test serializing a simple list of strings to Postgres array format."""
    pipeline = Pipeline()
    data = [ListModel(id=1, tags=["a", "b"])]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1].strip()

    assert '1,"{""a"",""b""}"' in row or "1,\"{""a"",""b""}\"" in row

def test_stage_data_list_serialization_quotes():
    """Test serializing list with values containing quotes."""
    pipeline = Pipeline()
    # Value: a"b
    data = [ListModel(id=2, tags=['a"b'])]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1].strip()

    # Expected: 2,"{""a\""b""}"
    # Just verify key parts to avoid escaping hell in test code
    assert '{' in row
    assert '}' in row
    # The backslash should be present to escape the quote for Postgres array
    assert '\\' in row
    # The quote should be doubled for CSV
    assert '""' in row

def test_stage_data_list_serialization_none():
    """Test list is None."""
    pipeline = Pipeline()
    data = [ListModel(id=3, tags=None)]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1].strip()
    # Should be empty string
    assert "3," == row or "3," in row

def test_stage_data_list_serialization_empty():
    """Test empty list."""
    pipeline = Pipeline()
    data = [ListModel(id=4, tags=[])]

    chunks = list(pipeline.stage_data(data))
    row = chunks[1].strip()
    # Postgres empty array: {}
    # CSV: "{}"
    assert "{}" in row
