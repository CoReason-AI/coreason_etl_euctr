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

import pytest
from loguru import logger

from coreason_etl_euctr.main import ETLConfig, parse_args, run_pipeline


@pytest.fixture  # type: ignore
def capture_logs() -> Generator[List[str], None, None]:
    """Fixture to capture loguru logs."""
    logs = []
    sink_id = logger.add(lambda msg: logs.append(msg))
    yield logs
    logger.remove(sink_id)


def test_parse_args_defaults() -> None:
    """Test that default arguments are set correctly (Delta mode)."""
    args = parse_args([])
    assert args.full is False


def test_parse_args_full() -> None:
    """Test that --full flag is parsed correctly."""
    args = parse_args(["--full"])
    assert args.full is True


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
