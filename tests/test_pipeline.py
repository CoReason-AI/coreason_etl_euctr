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
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.pipeline import Pipeline
from coreason_etl_euctr.storage import StorageBackend, StorageObject
from pydantic import BaseModel


class SimpleModel(BaseModel):
    id: str
    val: str
    items: list[str] | None = None


@pytest.fixture  # type: ignore[misc]
def temp_state_file(tmp_path: Path) -> Path:
    return tmp_path / "state.json"


def test_pipeline_load_state_missing(temp_state_file: Path) -> None:
    """Test loading state when file does not exist."""
    p = Pipeline(state_file=temp_state_file)
    assert p.load_state() == {}


def test_pipeline_load_state_corrupt(temp_state_file: Path) -> None:
    """Test loading state when file is corrupt JSON."""
    temp_state_file.write_text("{invalid", encoding="utf-8")
    p = Pipeline(state_file=temp_state_file)
    assert p.load_state() == {}


def test_pipeline_load_state_invalid_type(temp_state_file: Path) -> None:
    """Test loading state when JSON is not a dict."""
    temp_state_file.write_text("[]", encoding="utf-8")
    p = Pipeline(state_file=temp_state_file)
    assert p.load_state() == {}

def test_pipeline_load_state_read_error(temp_state_file: Path) -> None:
    """Test generic read error."""
    p = Pipeline(state_file=temp_state_file)
    # Ensure exists to pass first check
    temp_state_file.touch()
    with patch("pathlib.Path.read_text", side_effect=Exception("Read Error")):
        assert p.load_state() == {}

def test_pipeline_save_state(temp_state_file: Path) -> None:
    """Test saving state."""
    p = Pipeline(state_file=temp_state_file)
    state = {"foo": "bar"}
    p.save_state(state)

    assert temp_state_file.exists()
    content = json.loads(temp_state_file.read_text(encoding="utf-8"))
    assert content == state


def test_pipeline_save_state_error(temp_state_file: Path) -> None:
    """Test error handling during save."""
    p = Pipeline(state_file=temp_state_file)
    # Mock open to fail
    with patch("pathlib.Path.open", side_effect=Exception("Write Fail")):
        p.save_state({"a": 1})
    # Should log error but not crash
    assert not temp_state_file.exists()

def test_pipeline_save_cleanup_error(temp_state_file: Path) -> None:
    """Test error during cleanup of temp file."""
    p = Pipeline(state_file=temp_state_file)
    # Mock replace to fail (trigger cleanup), and unlink to fail (cleanup fail)
    # We need to ensure open succeeds so temp file is created
    with patch("pathlib.Path.replace", side_effect=Exception("Rename Fail")):
        with patch("pathlib.Path.unlink", side_effect=Exception("Unlink Fail")):
             p.save_state({"a": 1})
    # Should swallow cleanup error and log main error
    assert temp_state_file.with_suffix(".tmp").exists()


def test_high_water_mark(temp_state_file: Path) -> None:
    """Test getting/setting high water mark."""
    p = Pipeline(state_file=temp_state_file)
    assert p.get_high_water_mark() is None

    d = date(2023, 1, 1)
    p.set_high_water_mark(d)
    assert p.get_high_water_mark() == d

    # Test invalid date in file
    temp_state_file.write_text('{"last_updated": "bad-date"}', encoding="utf-8")
    assert p.get_high_water_mark() is None


def test_silver_watermark(temp_state_file: Path) -> None:
    """Test silver watermark (timestamp)."""
    p = Pipeline(state_file=temp_state_file)
    assert p.get_silver_watermark() is None

    ts = 123456.789
    p.set_silver_watermark(ts)
    assert p.get_silver_watermark() == ts

    # Test invalid
    temp_state_file.write_text('{"silver_last_run": "not-a-float"}', encoding="utf-8")
    assert p.get_silver_watermark() is None


def test_crawl_cursor(temp_state_file: Path) -> None:
    """Test crawl cursor (page number)."""
    p = Pipeline(state_file=temp_state_file)
    assert p.get_crawl_cursor() is None

    p.set_crawl_cursor(10)
    assert p.get_crawl_cursor() == 10

    # Test invalid
    temp_state_file.write_text('{"crawl_last_page": "ten"}', encoding="utf-8")
    assert p.get_crawl_cursor() is None


def test_stage_data_empty() -> None:
    """Test staging empty list."""
    p = Pipeline()
    gen = p.stage_data([])
    assert list(gen) == []


def test_stage_data_csv_formatting() -> None:
    """Test CSV generation with list formatting."""
    models = [
        SimpleModel(id="1", val="A", items=["x", "y"]),
        SimpleModel(id="2", val="B", items=None),
    ]
    p = Pipeline()
    gen = p.stage_data(models)

    output = list(gen)
    assert len(output) == 3 # Header + 2 rows

    header = output[0].strip()
    assert "id" in header

    row1 = output[1].strip()
    # Postgres array format: {x,y}
    # And CSV quoting: "1","A","{""x"",""y""}"
    # Note: Our implementation does:
    # array_str = "{" + ",".join(...) + "}"
    # row = [..., array_str]
    # writer.writerow quoted minimally.
    # Check for "{""x"",""y""}" inside the CSV line.
    assert '{""x"",""y""}' in row1 or '{"x","y"}' in row1

    row2 = output[2].strip()
    # None -> ""
    assert '"2","B",""' in row2 or '2,B,' in row2


def test_stage_data_deduplication() -> None:
    """Test that identical rows are skipped."""
    m1 = SimpleModel(id="1", val="A")
    m2 = SimpleModel(id="1", val="A") # Duplicate
    m3 = SimpleModel(id="2", val="B")

    p = Pipeline()
    gen = p.stage_data([m1, m2, m3])
    output = list(gen)

    # Header + 2 unique rows
    assert len(output) == 3


def test_pg_array_escape() -> None:
    """Test robust array escaping."""
    m = SimpleModel(id="1", val="A", items=['a"b', 'c\\d'])
    p = Pipeline()
    gen = p.stage_data([m])
    rows = list(gen)
    row = rows[1]
    assert '"{""a\\""b"",""c\\\\d""}"' in row

def test_identify_new_files(temp_state_file: Path) -> None:
    """Test identifying new files based on watermark."""
    # Mock storage
    mock_storage = MagicMock(spec=StorageBackend)
    files = [
        StorageObject(key="old.html", mtime=100.0),
        StorageObject(key="new.html", mtime=300.0),
        StorageObject(key="future.html", mtime=9999999999.9), # Way in future
    ]
    mock_storage.list_files.return_value = files

    p = Pipeline(state_file=temp_state_file, storage_backend=mock_storage)
    p.set_silver_watermark(200.0)

    # Freeze time to 1000.0
    with patch("time.time", return_value=1000.0):
        results = p.identify_new_files()

    keys = [f.key for f in results]
    assert "new.html" in keys
    assert "old.html" not in keys
    assert "future.html" not in keys
    assert len(keys) == 1

def test_identify_new_files_no_backend() -> None:
    """Test graceful handling of missing backend."""
    p = Pipeline() # No backend
    assert p.identify_new_files() == []
