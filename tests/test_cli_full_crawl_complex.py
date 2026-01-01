# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock

from coreason_etl_euctr.main import run_bronze
from coreason_etl_euctr.pipeline import Pipeline


def test_ignore_hwm_ignores_cursor() -> None:
    """
    Complex Case 1: ignore_hwm=True should ignore crawl_cursor and start at page 1.
    """
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = []
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock(spec=Pipeline)

    # Setup HWM and Cursor
    mock_pipeline.get_high_water_mark.return_value = "2023-01-01"
    mock_pipeline.get_crawl_cursor.return_value = 50

    run_bronze(
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
        ignore_hwm=True,
    )

    # Verify:
    # 1. HWM ignored (date_from=None)
    # 2. Cursor ignored (start_page=1, NOT 51)
    mock_crawler.harvest_ids.assert_called_once()
    kwargs = mock_crawler.harvest_ids.call_args.kwargs
    assert kwargs.get("date_from") is None
    assert kwargs.get("start_page") == 1


def test_ignore_hwm_respects_explicit_start_page() -> None:
    """
    Edge Case 2: ignore_hwm=True with explicit start_page should use explicit page.
    """
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = []
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock(spec=Pipeline)

    mock_pipeline.get_crawl_cursor.return_value = 50

    run_bronze(
        start_page=5,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
        ignore_hwm=True,
    )

    # Verify start_page is 5 (explicit) not 1 (default) and not 51 (cursor)
    kwargs = mock_crawler.harvest_ids.call_args.kwargs
    assert kwargs.get("start_page") == 5
    assert kwargs.get("date_from") is None


def test_normal_run_resumes_from_cursor() -> None:
    """
    Regression Case 3: ignore_hwm=False should resume from cursor.
    """
    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = []
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock(spec=Pipeline)

    mock_pipeline.get_crawl_cursor.return_value = 50

    run_bronze(
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
        ignore_hwm=False,
    )

    # Verify start_page is 51 (cursor + 1)
    kwargs = mock_crawler.harvest_ids.call_args.kwargs
    assert kwargs.get("start_page") == 51
