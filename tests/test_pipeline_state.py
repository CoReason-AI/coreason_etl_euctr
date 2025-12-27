# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import json
from datetime import date
from pathlib import Path
from typing import Generator

import pytest
from coreason_etl_euctr.pipeline import Pipeline


@pytest.fixture  # type: ignore[misc]
def temp_state_file(tmp_path: Path) -> Generator[Path, None, None]:
    """Fixture to provide a temporary state file path."""
    state_file = tmp_path / "state.json"
    yield state_file
    if state_file.exists():
        state_file.unlink()


def test_pipeline_init_default() -> None:
    """Test Pipeline initialization with default state file."""
    pipeline = Pipeline()
    # Check if state_file attribute exists and has default value
    # Note: We haven't implemented it yet, so this verifies the attribute is added.
    assert hasattr(pipeline, "state_file")
    assert pipeline.state_file == Path("data/state.json")


def test_pipeline_init_custom(temp_state_file: Path) -> None:
    """Test Pipeline initialization with custom state file."""
    pipeline = Pipeline(state_file=temp_state_file)
    assert pipeline.state_file == temp_state_file


def test_load_state_missing_file(temp_state_file: Path) -> None:
    """Test loading state when file does not exist returns empty dict."""
    pipeline = Pipeline(state_file=temp_state_file)
    state = pipeline.load_state()
    assert state == {}


def test_save_and_load_state(temp_state_file: Path) -> None:
    """Test saving and then loading state."""
    pipeline = Pipeline(state_file=temp_state_file)
    test_state = {"last_run": "2023-01-01", "count": 100}

    pipeline.save_state(test_state)
    assert temp_state_file.exists()

    loaded_state = pipeline.load_state()
    assert loaded_state == test_state


def test_get_high_water_mark_none(temp_state_file: Path) -> None:
    """Test getting high water mark when it's not set."""
    pipeline = Pipeline(state_file=temp_state_file)
    # Ensure clean state
    if temp_state_file.exists():
        temp_state_file.unlink()

    assert pipeline.get_high_water_mark() is None


def test_set_and_get_high_water_mark(temp_state_file: Path) -> None:
    """Test setting and retrieving the high water mark."""
    pipeline = Pipeline(state_file=temp_state_file)
    test_date = date(2023, 10, 25)

    pipeline.set_high_water_mark(test_date)

    # Verify it persists to disk immediately (or we can check memory if we decide to separate save)
    # The requirement assumes persistence.
    retrieved_date = pipeline.get_high_water_mark()
    assert retrieved_date == test_date

    # Verify file content
    content = json.loads(temp_state_file.read_text())
    assert content.get("last_updated") == "2023-10-25"


def test_load_state_corrupted_json(temp_state_file: Path) -> None:
    """Test loading state with corrupted JSON file."""
    temp_state_file.write_text("{invalid_json")
    pipeline = Pipeline(state_file=temp_state_file)

    # Should return empty dict or raise error?
    # Robustness suggests returning empty dict and logging warning,
    # but for ETL critical path, maybe raising is safer?
    # Let's assume we want to reset or return empty for resilience,
    # but logging is hard to test without caplog.
    # We'll assert it returns empty dict to be safe.
    state = pipeline.load_state()
    assert state == {}


def test_get_high_water_mark_invalid_format(temp_state_file: Path) -> None:
    """Test getting high water mark when date string is invalid."""
    data = {"last_updated": "not-a-date"}
    temp_state_file.write_text(json.dumps(data))

    pipeline = Pipeline(state_file=temp_state_file)
    # Should handle parsing error and return None
    assert pipeline.get_high_water_mark() is None


def test_load_state_invalid_type(temp_state_file: Path) -> None:
    """Test loading state when file contains valid JSON but not a dict."""
    temp_state_file.write_text("[1, 2, 3]")
    pipeline = Pipeline(state_file=temp_state_file)
    state = pipeline.load_state()
    assert state == {}


def test_load_state_generic_error(temp_state_file: Path) -> None:
    """Test loading state when a generic exception occurs."""
    pipeline = Pipeline(state_file=temp_state_file)
    # Create the file so it exists
    temp_state_file.touch()

    # Mock read_text to raise exception
    # We need to patch the instance's state_file.read_text, but state_file is a Path object.
    # It's easier to patch pathlib.Path.read_text globally or stick a mock object.
    # Since we can't easily swap the Path object after init without being invasive,
    # let's try patching pathlib.Path.read_text.

    from unittest.mock import patch

    with patch("pathlib.Path.read_text", side_effect=PermissionError("Boom")):
        state = pipeline.load_state()
        assert state == {}


def test_save_state_generic_error(temp_state_file: Path) -> None:
    """Test saving state when a generic exception occurs."""
    pipeline = Pipeline(state_file=temp_state_file)

    from unittest.mock import patch

    # Patch open to fail
    with patch("pathlib.Path.open", side_effect=PermissionError("Boom")):
        # Should catch error and log it, not raise
        pipeline.save_state({"a": 1})
