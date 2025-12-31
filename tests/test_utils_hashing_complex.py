# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import unicodedata

from coreason_etl_euctr.utils.hashing import compute_content_hash


def test_hashing_large_payload() -> None:
    """Test hashing of a significantly large string (5MB)."""
    # 5MB of repeated 'a'
    large_content = "a" * (5 * 1024 * 1024)
    result = compute_content_hash(large_content)
    # Just ensure it computes and returns a valid SHA256 hex string
    assert len(result) == 64
    assert isinstance(result, str)


def test_hashing_null_bytes() -> None:
    """Test hashing of strings containing null bytes."""
    content = "hello\0world"
    result = compute_content_hash(content)
    # Check against known hash for "hello\0world"
    # echo -n -e "hello\x00world" | sha256sum
    expected = "b206899bc103669c8e7b36de29d73f95b46795b508aa87d612b2ce84bfb29df2"
    assert result == expected


def test_hashing_sensitivity() -> None:
    """Test that a single character difference produces a completely different hash."""
    content1 = "The quick brown fox jumps over the lazy dog"
    content2 = "The quick brown fox jumps over the lazy hog"

    hash1 = compute_content_hash(content1)
    hash2 = compute_content_hash(content2)

    assert hash1 != hash2
    # Ensure no obvious similarity (avalanche effect)
    assert hash1[:10] != hash2[:10]


def test_hashing_unicode_normalization() -> None:
    """
    Test that visually identical strings with different byte representations
    produce DIFFERENT hashes (since we hash the UTF-8 bytes).
    """
    # 'é' can be represented as U+00E9 (NFC) or U+0065 U+0301 (NFD)
    char_nfc = "\u00e9"
    char_nfd = "\u0065\u0301"

    # Visually they are the same
    assert char_nfc == unicodedata.normalize("NFC", char_nfd)

    # But their hashes should differ because the input string bytes differ
    hash_nfc = compute_content_hash(char_nfc)
    hash_nfd = compute_content_hash(char_nfd)

    assert hash_nfc != hash_nfd
