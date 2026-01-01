# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.main import run_bronze
from coreason_etl_euctr.pipeline import Pipeline


def test_resume_scenario_interruption_at_page_3(tmp_path: Path) -> None:
    """
    SCENARIO: Interruption at Page 3.
    1. Run 1: Start at page 1. Page 1, 2 success. Page 3 fails (Exception).
       Expected State: cursor = 2.
    2. Run 2: Restart (default start_page=1). Reads state. Resumes at 3.
       Page 3 success.
       Expected State: cursor = 3.
    """
    output_dir = tmp_path / "bronze"
    output_dir.mkdir()
    state_file = tmp_path / "state.json"

    # Use real Pipeline with temp state file to verify persistence
    pipeline = Pipeline(state_file=state_file)

    # Mock Crawler
    mock_client = MagicMock(spec=httpx.Client)
    crawler = Crawler(client=mock_client)

    mock_downloader = MagicMock()
    mock_downloader.download_trial.return_value = True

    # --- RUN 1 ---
    # Page 1: OK, Page 2: OK, Page 3: Exception
    page1_html = "<div><span>EudraCT Number:</span> <span>ID-1</span></div>"
    page2_html = "<div><span>EudraCT Number:</span> <span>ID-2</span></div>"

    with pytest.raises(Exception, match="Crash on Page 3"):
        with patch.object(crawler, "fetch_search_page") as mock_fetch:
            mock_fetch.side_effect = [page1_html, page2_html, Exception("Crash on Page 3")]

            run_bronze(
                output_dir=str(output_dir),
                start_page=1,
                max_pages=5,  # Intended to go further
                crawler=crawler,
                downloader=mock_downloader,
                pipeline=pipeline,
            )

    # Verify State after Crash
    cursor = pipeline.get_crawl_cursor()
    assert cursor == 2, "Cursor should be at last successful page (2)"

    # Verify IDs collected so far
    ids_file = output_dir / "ids.csv"
    content = ids_file.read_text(encoding="utf-8")
    assert "ID-1" in content
    assert "ID-2" in content
    assert "ID-3" not in content  # ID-3 wasn't fetched

    # --- RUN 2 ---
    # Resume. User runs without args (start_page=1 default).
    # Logic should pick up cursor=2, so start at 3.

    page3_html = "<div><span>EudraCT Number:</span> <span>ID-3</span></div>"

    with patch.object(crawler, "fetch_search_page") as mock_fetch:
        mock_fetch.side_effect = [page3_html]

        run_bronze(
            output_dir=str(output_dir),
            start_page=1,  # Default
            max_pages=1,  # Just process one more page
            crawler=crawler,
            downloader=mock_downloader,
            pipeline=pipeline,
        )

        # Verify it requested page 3
        args, kwargs = mock_fetch.call_args
        assert kwargs["page_num"] == 3

    # Verify State Updated
    cursor = pipeline.get_crawl_cursor()
    assert cursor == 3


def test_resume_scenario_immediate_failure(tmp_path: Path) -> None:
    """
    SCENARIO: Immediate Failure at Page 1.
    1. Run 1: Fails at Page 1.
       Expected State: cursor is None (or 0 if logic allows, but likely None).
    2. Run 2: Start at 1. Succeeds.
    """
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)
    mock_client = MagicMock(spec=httpx.Client)
    crawler = Crawler(client=mock_client)
    mock_downloader = MagicMock()

    # --- RUN 1 ---
    with pytest.raises(Exception, match="Crash Start"):
        with patch.object(crawler, "fetch_search_page", side_effect=Exception("Crash Start")):
            run_bronze(
                output_dir=str(tmp_path),
                start_page=1,
                max_pages=1,
                crawler=crawler,
                downloader=mock_downloader,
                pipeline=pipeline,
            )

    # Verify State
    cursor = pipeline.get_crawl_cursor()
    assert cursor is None

    # --- RUN 2 ---
    page1_html = "<div><span>EudraCT Number:</span> <span>ID-1</span></div>"
    with patch.object(crawler, "fetch_search_page", return_value=page1_html) as mock_fetch:
        run_bronze(
            output_dir=str(tmp_path),
            start_page=1,
            max_pages=1,
            crawler=crawler,
            downloader=mock_downloader,
            pipeline=pipeline,
        )

        args, kwargs = mock_fetch.call_args
        assert kwargs["page_num"] == 1  # Starts at 1

    assert pipeline.get_crawl_cursor() == 1


def test_scenario_max_pages_boundary(tmp_path: Path) -> None:
    """
    SCENARIO: Boundary Condition.
    Request max_pages=3. P1, P2 OK. P3 OK.
    State should be start_page + 3 - 1.
    """
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)
    mock_client = MagicMock(spec=httpx.Client)
    crawler = Crawler(client=mock_client)
    mock_downloader = MagicMock()

    page_html = "<div><span>EudraCT Number:</span> <span>ID-X</span></div>"

    with patch.object(crawler, "fetch_search_page", return_value=page_html):
        run_bronze(
            output_dir=str(tmp_path),
            start_page=10,
            max_pages=3,
            crawler=crawler,
            downloader=mock_downloader,
            pipeline=pipeline,
        )

    # Processed 10, 11, 12.
    assert pipeline.get_crawl_cursor() == 12


def test_scenario_empty_page_stop(tmp_path: Path) -> None:
    """
    SCENARIO: Empty page encountered before max_pages.
    Request 5 pages. Page 1 has data. Page 2 is empty (no IDs).
    Should stop. Cursor should be 1? Or 2?
    If Page 2 was successfully fetched but had no IDs, is it "processed"?
    Crawler.harvest_ids logic:
        if not ids: log warning, break.
        Does NOT yield (i, ids).
    So loop in run_bronze does NOT execute body for Page 2.
    So cursor remains at 1.

    This is correct because if we resume, we might want to check Page 2 again
    (maybe data appeared), or maybe we consider it done.
    But strictly speaking, if we didn't harvest anything, we didn't "finish"
    getting data from it in the sense of finding trials.
    However, if it's empty, we probably don't want to loop forever on it.
    But given the logic, restarting at 2 is fine, it will just be empty again and stop again.
    """
    state_file = tmp_path / "state.json"
    pipeline = Pipeline(state_file=state_file)
    mock_client = MagicMock(spec=httpx.Client)
    crawler = Crawler(client=mock_client)
    mock_downloader = MagicMock()

    page1_html = "<div><span>EudraCT Number:</span> <span>ID-1</span></div>"
    page2_html = "<html>No results</html>"

    with patch.object(crawler, "fetch_search_page") as mock_fetch:
        mock_fetch.side_effect = [page1_html, page2_html]

        run_bronze(
            output_dir=str(tmp_path),
            start_page=1,
            max_pages=5,
            crawler=crawler,
            downloader=mock_downloader,
            pipeline=pipeline,
        )

    # Should have processed page 1. Page 2 fetched but yielded nothing.
    assert pipeline.get_crawl_cursor() == 1
