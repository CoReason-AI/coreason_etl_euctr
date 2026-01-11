# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import patch

from coreason_etl_euctr.main import main


def test_cli_crawl_defaults() -> None:
    """Test the crawl command with default arguments."""
    with patch("sys.argv", ["euctr-etl", "crawl"]), patch("coreason_etl_euctr.main.run_bronze") as mock_run:
        ret = main()
        assert ret == 0
        mock_run.assert_called_once_with(
            output_dir="data/bronze",
            start_page=1,
            max_pages=1,
            storage_backend=None,
            ignore_hwm=False,
            sleep_seconds=1.0,
            country_priority=["3rd", "GB", "DE"],
        )


def test_cli_crawl_custom_args() -> None:
    """Test the crawl command with custom arguments."""
    with (
        patch(
            "sys.argv",
            [
                "euctr-etl",
                "crawl",
                "--output-dir",
                "custom/dir",
                "--start-page",
                "5",
                "--max-pages",
                "10",
            ],
        ),
        patch("coreason_etl_euctr.main.run_bronze") as mock_run,
    ):
        ret = main()
        assert ret == 0
        mock_run.assert_called_once_with(
            output_dir="custom/dir",
            start_page=5,
            max_pages=10,
            storage_backend=None,
            ignore_hwm=False,
            sleep_seconds=1.0,
            country_priority=["3rd", "GB", "DE"],
        )


def test_cli_crawl_config_args() -> None:
    """Test the crawl command with configuration arguments."""
    with (
        patch(
            "sys.argv",
            [
                "euctr-etl",
                "crawl",
                "--sleep-seconds",
                "0.5",
                "--country-priority",
                "FR",
                "ES",
            ],
        ),
        patch("coreason_etl_euctr.main.run_bronze") as mock_run,
    ):
        ret = main()
        assert ret == 0
        mock_run.assert_called_once_with(
            output_dir="data/bronze",
            start_page=1,
            max_pages=1,
            storage_backend=None,
            ignore_hwm=False,
            sleep_seconds=0.5,
            country_priority=["FR", "ES"],
        )


def test_cli_load_defaults() -> None:
    """Test the load command with default arguments."""
    with patch("sys.argv", ["euctr-etl", "load"]), patch("coreason_etl_euctr.main.run_silver") as mock_run:
        ret = main()
        assert ret == 0
        # Check arguments but ignore loader instance equality
        args, kwargs = mock_run.call_args
        assert kwargs["input_dir"] == "data/bronze"
        assert kwargs["mode"] == "FULL"
        assert kwargs["storage_backend"] is None
        assert kwargs["loader"] is not None


def test_cli_load_custom_args() -> None:
    """Test the load command with custom arguments."""
    with (
        patch(
            "sys.argv",
            ["euctr-etl", "load", "--input-dir", "custom/bronze", "--mode", "UPSERT"],
        ),
        patch("coreason_etl_euctr.main.run_silver") as mock_run,
    ):
        ret = main()
        assert ret == 0
        args, kwargs = mock_run.call_args
        assert kwargs["input_dir"] == "custom/bronze"
        assert kwargs["mode"] == "UPSERT"
        assert kwargs["storage_backend"] is None
        assert kwargs["loader"] is not None


def test_cli_help() -> None:
    """Test that calling without arguments prints help and returns 1."""
    with patch("sys.argv", ["euctr-etl"]), patch("argparse.ArgumentParser.print_help") as mock_print:
        ret = main()
        assert ret == 1
        mock_print.assert_called_once()
