# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import sys
from datetime import date
from unittest.mock import MagicMock, patch

from coreason_etl_euctr.main import main, run_bronze
from coreason_etl_euctr.pipeline import Pipeline


def test_run_bronze_ignores_hwm() -> None:
    """
    Test that run_bronze ignores the High-Water Mark when ignore_hwm is True.
    """
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = []
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock(spec=Pipeline)

    # Setup HWM in pipeline
    mock_pipeline.get_high_water_mark.return_value = date(2023, 1, 1)

    # Call run_bronze with ignore_hwm=True
    run_bronze(
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
        ignore_hwm=True,
    )

    # Verify pipeline.get_high_water_mark was NOT called (or result ignored)
    # Actually our implementation MIGHT call it if we structured the if condition poorly,
    # but the key is that harvest_ids receives date_from=None.

    mock_crawler.harvest_ids.assert_called_once()
    call_kwargs = mock_crawler.harvest_ids.call_args.kwargs
    assert call_kwargs.get("date_from") is None


def test_run_bronze_uses_hwm_default() -> None:
    """
    Test that run_bronze USES the High-Water Mark when ignore_hwm is False (default).
    """
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = []
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock(spec=Pipeline)

    # Setup HWM in pipeline
    mock_pipeline.get_high_water_mark.return_value = date(2023, 1, 1)

    # Call run_bronze with ignore_hwm=False
    run_bronze(
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
        ignore_hwm=False,
    )

    mock_crawler.harvest_ids.assert_called_once()
    call_kwargs = mock_crawler.harvest_ids.call_args.kwargs
    assert call_kwargs.get("date_from") == "2023-01-01"


def test_cli_passes_ignore_hwm_flag() -> None:
    """
    Test that the CLI correctly parses --ignore-hwm and passes it to run_bronze.
    """
    test_args = ["euctr-etl", "crawl", "--ignore-hwm"]

    with patch.object(sys, "argv", test_args):
        with patch("coreason_etl_euctr.main.run_bronze") as mock_run_bronze:
            with patch("coreason_etl_euctr.main.Crawler"):
                with patch("coreason_etl_euctr.main.Downloader"):
                    main()

                    mock_run_bronze.assert_called_once()
                    call_kwargs = mock_run_bronze.call_args.kwargs
                    assert call_kwargs.get("ignore_hwm") is True


def test_cli_default_ignore_hwm_flag() -> None:
    """
    Test that the CLI defaults ignore_hwm to False.
    """
    test_args = ["euctr-etl", "crawl"]

    with patch.object(sys, "argv", test_args):
        with patch("coreason_etl_euctr.main.run_bronze") as mock_run_bronze:
            with patch("coreason_etl_euctr.main.Crawler"):
                with patch("coreason_etl_euctr.main.Downloader"):
                    main()

                    mock_run_bronze.assert_called_once()
                    call_kwargs = mock_run_bronze.call_args.kwargs
                    assert call_kwargs.get("ignore_hwm") is False
