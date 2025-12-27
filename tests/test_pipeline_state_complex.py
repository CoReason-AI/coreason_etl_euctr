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
from unittest.mock import patch

import pytest
from coreason_etl_euctr.pipeline import Pipeline


def test_directory_auto_creation(tmp_path: Path) -> None:
    """Test that saving state creates necessary parent directories."""
    # Define a deep path that doesn't exist
    deep_path = tmp_path / "subdir" / "deep" / "state.json"
    pipeline = Pipeline(state_file=deep_path)

    state = {"test": "value"}
    pipeline.save_state(state)

    assert deep_path.exists()
    assert deep_path.parent.exists()
    assert json.loads(deep_path.read_text()) == state


def test_state_key_preservation(tmp_path: Path) -> None:
    """Test that setting high water mark preserves other state keys."""
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)

    # Initial state with extra metadata
    initial_state = {
        "run_count": 42,
        "config": {"mode": "delta"},
        "last_updated": "2020-01-01"
    }
    state_file.write_text(json.dumps(initial_state))

    # Update high water mark
    new_date = date(2023, 12, 25)
    pipeline.set_high_water_mark(new_date)

    # Reload and verify
    loaded_state = json.loads(state_file.read_text())
    assert loaded_state["run_count"] == 42
    assert loaded_state["config"] == {"mode": "delta"}
    assert loaded_state["last_updated"] == "2023-12-25"


def test_unicode_support(tmp_path: Path) -> None:
    """Test saving and loading state with Unicode characters."""
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)

    # Unicode content (e.g. Greek, Emoji, Kanji)
    unicode_state = {
        "message": "Γειά σου Κόσμε",
        "emoji": "💊",
        "kanji": "臨床試験"
    }

    pipeline.save_state(unicode_state)

    # Verify file content is UTF-8
    content = state_file.read_bytes()
    assert b"\xce\x93" in content  # Gamma

    # Load back
    loaded = pipeline.load_state()
    assert loaded == unicode_state


def test_atomic_write_failure(tmp_path: Path) -> None:
    """
    Test that if a write fails, the original file is preserved.
    This simulates an atomic write requirement.
    """
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)

    # Establish original valid state
    original_state = {"status": "ok"}
    state_file.write_text(json.dumps(original_state))

    # We want to simulate a failure DURING the write.
    # If using direct open('w'), the file is truncated immediately.
    # If using atomic write (tmp + rename), the failure happens writing tmp,
    # and original is untouched.

    # We mock json.dump to raise an error.
    # If implementation is naive (open w -> dump), file is already truncated
    # when dump runs (or at least opened).
    # Actually, 'w' truncates on open.

    with patch("json.dump", side_effect=RuntimeError("Disk full")):
        pipeline.save_state({"status": "corrupt"})

    # Check if original file is still intact
    # Current implementation: it fails. 'w' mode truncates.
    # So assertions will fail if not atomic.

    if not state_file.exists():
        pytest.fail("State file was deleted/lost during failed write!")

    content = state_file.read_text()
    # If naive implementation, content might be empty
    if content == "":
        pytest.fail("State file was truncated (emptied) during failed write!")

    assert json.loads(content) == original_state


def test_save_state_cleanup_failure(tmp_path: Path) -> None:
    """
    Test that if cleanup fails after a write failure, it is ignored (safe).
    """
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)

    # We need to trigger the outer exception (write fail) AND inner exception (cleanup fail)

    with patch("json.dump", side_effect=RuntimeError("Write Fail")):
        with patch("pathlib.Path.unlink", side_effect=OSError("Cleanup Fail")):
            # This should log the error but not raise
            pipeline.save_state({"a": 1})
