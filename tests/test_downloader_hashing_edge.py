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

import pytest
from coreason_etl_euctr.downloader import Downloader
from coreason_etl_euctr.utils.hashing import compute_content_hash


@pytest.fixture  # type: ignore[misc]
def mock_storage() -> MagicMock:
    return MagicMock()


@pytest.fixture  # type: ignore[misc]
def downloader(mock_storage: MagicMock) -> Downloader:
    return Downloader(storage_backend=mock_storage)


def test_save_content_idempotency_skip_write(downloader: Downloader, mock_storage: MagicMock) -> None:
    """
    Test that if hash matches existing metadata, we skip writing the HTML file (optimization),
    but we DO update the metadata (timestamp/freshness check).
    """
    trial_id = "2020-001234-56"
    content = "<html>No Change</html>"
    country = "GB"

    content_hash = compute_content_hash(content)

    # Mock existence of .meta file
    mock_storage.exists.return_value = True
    # Mock content of .meta file with MATCHING hash
    mock_storage.read.return_value = f"source_country=GB\nhash={content_hash}"

    # Action
    downloader._save_content(trial_id, content, country)

    # Verification
    # 1. Ensure HTML file write was SKIPPED
    html_calls = [args for args, _ in mock_storage.write.call_args_list if args[0] == f"{trial_id}.html"]
    assert len(html_calls) == 0, "Should not write HTML file if hash matches"

    # 2. Ensure Meta file write was PERFORMED (to update timestamp)
    meta_calls = [args for args, _ in mock_storage.write.call_args_list if args[0] == f"{trial_id}.meta"]
    assert len(meta_calls) == 1, "Should update metadata even if hash matches"

    # Verify hash is in the new metadata
    saved_meta = meta_calls[0][1]
    assert f"hash={content_hash}" in saved_meta


def test_save_content_hash_mismatch_overwrites(downloader: Downloader, mock_storage: MagicMock) -> None:
    """
    Test that if hash differs from existing metadata, we overwrite both HTML and Meta.
    """
    trial_id = "2020-001234-56"
    content = "<html>New Content</html>"
    country = "GB"

    new_hash = compute_content_hash(content)
    old_hash = "old_hash_value"

    mock_storage.exists.return_value = True
    mock_storage.read.return_value = f"source_country=GB\nhash={old_hash}"

    downloader._save_content(trial_id, content, country)

    # Verify HTML Write happened
    mock_storage.write.assert_any_call(f"{trial_id}.html", content)

    # Verify Meta Write happened with new hash
    # We can check the last call or search for the meta key
    meta_call_args = [args for args, _ in mock_storage.write.call_args_list if args[0] == f"{trial_id}.meta"]
    assert len(meta_call_args) == 1
    assert f"hash={new_hash}" in meta_call_args[0][1]


def test_save_content_missing_hash_key_overwrites(downloader: Downloader, mock_storage: MagicMock) -> None:
    """
    Test that if metadata exists but lacks the 'hash' key (legacy data), we overwrite.
    """
    trial_id = "2020-001234-56"
    content = "<html>Content</html>"
    country = "GB"

    mock_storage.exists.return_value = True
    # Meta exists but no hash
    mock_storage.read.return_value = "source_country=GB\ndownloaded_at=12345"

    downloader._save_content(trial_id, content, country)

    # Verify HTML Write happened
    mock_storage.write.assert_any_call(f"{trial_id}.html", content)


def test_save_content_corrupted_metadata_overwrites(downloader: Downloader, mock_storage: MagicMock) -> None:
    """
    Test that if metadata read raises an exception (corrupted/permissions), we safely overwrite.
    """
    trial_id = "2020-001234-56"
    content = "<html>Content</html>"
    country = "GB"

    mock_storage.exists.return_value = True
    # Simulate read error
    mock_storage.read.side_effect = Exception("Corrupted file")

    downloader._save_content(trial_id, content, country)

    # Verify HTML Write happened
    mock_storage.write.assert_any_call(f"{trial_id}.html", content)
    # Verify Meta Write happened
    # Check that exception was caught and ignored
