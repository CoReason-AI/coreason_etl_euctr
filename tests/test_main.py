# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import Generator, List
from unittest.mock import patch

import pytest
from loguru import logger

from coreason_etl_euctr.main import (
    ETLConfig,
    main,
    parse_args,
    run_pipeline,
    setup_logging,
)


@pytest.fixture  # type: ignore
def capture_logs() -> Generator[List[str], None, None]:
    """Fixture to capture loguru logs."""
    logs = []
    sink_id = logger.add(lambda msg: logs.append(msg))
    yield logs
    try:
        logger.remove(sink_id)
    except ValueError:
        # Handler might have been removed by the code under test
        pass


def test_parse_args_defaults() -> None:
    """Test that default arguments are set correctly (Delta mode)."""
    args = parse_args([])
    assert args.full is False


def test_parse_args_full() -> None:
    """Test that --full flag is parsed correctly."""
    args = parse_args(["--full"])
    assert args.full is True


def test_parse_args_invalid() -> None:
    """Test that invalid arguments raise SystemExit."""
    with pytest.raises(SystemExit):
        parse_args(["--invalid-flag"])


def test_etl_config_init() -> None:
    """Test configuration initialization."""
    config = ETLConfig(full_load=True)
    assert config.full_load is True
    assert config.target_countries == ["3rd", "GB", "DE"]


def test_run_pipeline_execution(capture_logs: List[str]) -> None:
    """Test that the pipeline runs and logs the correct mode."""
    config = ETLConfig(full_load=True)
    run_pipeline(config)

    assert any("Starting ETL Pipeline in FULL mode" in msg for msg in capture_logs)
    assert any("Triggering Full Re-crawl" in msg for msg in capture_logs)


def test_run_pipeline_delta_execution(capture_logs: List[str]) -> None:
    """Test that the pipeline runs in delta mode by default."""
    config = ETLConfig(full_load=False)
    run_pipeline(config)

    assert any("Starting ETL Pipeline in DELTA mode" in msg for msg in capture_logs)
    assert any("Triggering Delta Load" in msg for msg in capture_logs)


def test_setup_logging_valid() -> None:
    """Test logging setup with a valid level."""
    setup_logging("DEBUG")


def test_setup_logging_invalid(capsys) -> None:  # type: ignore
    """Test logging setup with an invalid level (should fallback)."""
    # setup_logging removes handlers and adds sys.stderr.
    # Since we are inside a test with capsys, sys.stderr is the capture stream.
    setup_logging("INVALID_LEVEL")

    captured = capsys.readouterr()
    assert "Invalid LOG_LEVEL 'INVALID_LEVEL' specified. Defaulting to INFO." in captured.err


def test_main_unhandled_exception(capsys) -> None:  # type: ignore
    """Test that main catches unhandled exceptions."""
    with patch("coreason_etl_euctr.main.parse_args") as mock_args:
        # Simulate a crash
        mock_args.side_effect = Exception("Boom!")

        # main calls sys.exit(1) on error
        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1

        captured = capsys.readouterr()
        assert "Unhandled exception: Boom!" in captured.err


def test_main_success() -> None:
    """Test main execution path without errors."""
    with (
        patch("sys.argv", ["script_name", "--full"]),
        patch("coreason_etl_euctr.main.run_pipeline") as mock_run,
        # Mock setup_logging to avoid messing with global logger state during this test if needed,
        # but leaving it real ensures integration coverage.
    ):
        main()
        mock_run.assert_called_once()
        # Check if config was passed correctly
        args, _ = mock_run.call_args
        assert args[0].full_load is True
