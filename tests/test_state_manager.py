# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from coreason_etl_euctr.utils.state_manager import EpistemicStateManagerTask


def test_state_manager_empty(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    manager = EpistemicStateManagerTask(state_file_path=str(state_file))

    assert manager.last_run_timestamp is None
    assert manager.get_hash("123") is None


def test_state_manager_persistence(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    manager1 = EpistemicStateManagerTask(state_file_path=str(state_file))

    manager1.last_run_timestamp = "2024-01-01"
    manager1.update_hash("123", "hash123")

    manager2 = EpistemicStateManagerTask(state_file_path=str(state_file))

    assert manager2.last_run_timestamp == "2024-01-01"
    assert manager2.get_hash("123") == "hash123"


def test_state_manager_invalid_json(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("{invalid json}")

    manager = EpistemicStateManagerTask(state_file_path=str(state_file))
    assert manager.last_run_timestamp is None


def test_state_manager_invalid_schema(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text('{"last_run_timestamp": 123}')  # Invalid type

    manager = EpistemicStateManagerTask(state_file_path=str(state_file))
    assert manager.last_run_timestamp is None


def test_state_manager_save_error(mocker: MockerFixture, tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    manager = EpistemicStateManagerTask(state_file_path=str(state_file))

    mock_logger_error = mocker.patch("coreason_etl_euctr.utils.state_manager.logger.error")
    mocker.patch("builtins.open", side_effect=Exception("mocked error"))

    with pytest.raises(Exception, match="mocked error"):
        manager.last_run_timestamp = "2024-01-01"

    mock_logger_error.assert_called_once()
