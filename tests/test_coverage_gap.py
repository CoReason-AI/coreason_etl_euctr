# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock, patch

from coreason_etl_euctr.crawler import Crawler
from coreason_etl_euctr.worker import process_file_content


def test_crawler_extract_ids_edge_cases() -> None:
    """Test edge cases in extract_ids to improve coverage."""
    crawler = Crawler()

    # 1. Label exists but no ID found (e.g. empty text)
    # Covers: if not trial_id: continue
    html_no_id = "<div><span>EudraCT Number:</span></div>"
    assert crawler.extract_ids(html_no_id) == []

    # 2. Label exists, parent has no siblings, text is just label
    # Covers: _extract_id_from_label returning None
    html_just_label = "<div><span>EudraCT Number:</span></div>"
    assert crawler.extract_ids(html_just_label) == []

    # 3. Label text matches but split fails/empty?
    # "EudraCT Number:   " -> split -> [] -> cleaned empty
    html_empty_val = "<div>EudraCT Number:   </div>"
    assert crawler.extract_ids(html_empty_val) == []

    # 4. Sibling exists but empty
    html_empty_sibling = "<div><span>EudraCT Number:</span> <span> </span></div>"
    assert crawler.extract_ids(html_empty_sibling) == []


def test_worker_storage_creation_failure() -> None:
    """Test worker failing to create storage backend."""
    # We pass a config that causes create_storage_backend to raise
    # e.g. unknown type
    config = {"type": "unknown"}
    result = process_file_content("key", config)
    assert result is None


def test_worker_read_failure() -> None:
    """Test worker failing to read file."""
    config = {"type": "mock"}

    mock_storage = MagicMock()
    mock_storage.read.side_effect = Exception("Read Error")

    with patch("coreason_etl_euctr.worker.create_storage_backend", return_value=mock_storage):
        result = process_file_content("key", config)

    assert result is None
