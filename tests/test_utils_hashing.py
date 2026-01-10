# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import pytest
from coreason_etl_euctr.utils.hashing import compute_content_hash


def test_compute_content_hash_basic() -> None:
    """Test standard string hashing."""
    content = "Hello World"
    # echo -n "Hello World" | sha256sum
    expected = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
    assert compute_content_hash(content) == expected


def test_compute_content_hash_empty() -> None:
    """Test hashing of empty string."""
    content = ""
    # echo -n "" | sha256sum
    expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert compute_content_hash(content) == expected


def test_compute_content_hash_unicode() -> None:
    """Test hashing of unicode characters."""
    content = "Héllo Wörld €"
    # Ensure it works and returns valid length hex
    hashed = compute_content_hash(content)
    assert len(hashed) == 64
    assert all(c in "0123456789abcdef" for c in hashed)


def test_compute_content_hash_none() -> None:
    """Test that None input raises ValueError."""
    with pytest.raises(ValueError, match="Content cannot be None"):
        compute_content_hash(None)  # type: ignore
