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
from unittest.mock import MagicMock, call

from coreason_etl_euctr.main import run_bronze


def test_bronze_resume_persistence(tmp_path: Path) -> None:
    """
    Verify that `run_bronze` appends IDs to the intermediate file across multiple runs,
    simulating a resume capability.
    """
    output_dir = tmp_path / "bronze"
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.get_high_water_mark.return_value = None

    # Run 1: Crawl Page 1, finds ID "A"
    mock_crawler.harvest_ids.return_value = iter([(1, [("A", None)])])
    # Setup crawl cursor check (return None)
    mock_pipeline.get_crawl_cursor.return_value = None

    run_bronze(
        output_dir=str(output_dir),
        start_page=1,
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Check file content
    ids_file = output_dir / "ids.csv"
    assert ids_file.exists()
    assert ids_file.read_text(encoding="utf-8").strip() == "A"

    # Verify Download A
    mock_downloader.download_trial.assert_called_once_with("A")
    mock_downloader.reset_mock()

    # Run 2: Crawl Page 2, finds ID "B"
    mock_crawler.harvest_ids.return_value = iter([(2, [("B", None)])])

    run_bronze(
        output_dir=str(output_dir),
        start_page=2,
        max_pages=1,
        crawler=mock_crawler,
        downloader=mock_downloader,
        pipeline=mock_pipeline,
    )

    # Check file content (should have A and B)
    content = ids_file.read_text(encoding="utf-8").splitlines()
    assert "A" in content
    assert "B" in content

    # Verify Download: It reads the file, so it sees A and B.
    # It should try to download both.
    assert mock_downloader.download_trial.call_count == 2
    mock_downloader.download_trial.assert_has_calls([call("A"), call("B")], any_order=True)


def test_bronze_deduplication_across_runs(tmp_path: Path) -> None:
    """
    Verify that if the same ID appears in the file multiple times (due to overlapping crawls),
    it is deduplicated before downloading.
    """
    output_dir = tmp_path / "bronze"
    mock_crawler = MagicMock()
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()

    # Pre-populate file with ID "A"
    output_dir.mkdir(parents=True)
    (output_dir / "ids.csv").write_text("A\n")

    # Run: Crawl Page 1, finds "A" again (and "B")
    mock_crawler.harvest_ids.return_value = iter([(1, [("A", None), ("B", None)])])
    mock_pipeline.get_crawl_cursor.return_value = None

    run_bronze(output_dir=str(output_dir), crawler=mock_crawler, downloader=mock_downloader, pipeline=mock_pipeline)

    # File should have A, A, B
    content = (output_dir / "ids.csv").read_text(encoding="utf-8").splitlines()
    assert content.count("A") == 2
    assert "B" in content

    # Download should be called once for A and once for B
    assert mock_downloader.download_trial.call_count == 2
    mock_downloader.download_trial.assert_has_calls([call("A"), call("B")], any_order=True)


def test_bronze_corrupted_id_file(tmp_path: Path) -> None:
    """
    Verify robustness against corrupted lines (empty, whitespace) in ids.csv.
    """
    output_dir = tmp_path / "bronze"
    output_dir.mkdir(parents=True)

    # Create corrupted file
    # "A", empty line, whitespace line, "B"
    (output_dir / "ids.csv").write_text("A\n\n   \nB\n")

    mock_crawler = MagicMock()
    mock_crawler.harvest_ids.return_value = iter([])  # Find nothing new
    mock_downloader = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.get_crawl_cursor.return_value = None

    run_bronze(output_dir=str(output_dir), crawler=mock_crawler, downloader=mock_downloader, pipeline=mock_pipeline)

    # Should identify A and B
    mock_downloader.download_trial.assert_has_calls([call("A"), call("B")], any_order=True)
    assert mock_downloader.download_trial.call_count == 2
