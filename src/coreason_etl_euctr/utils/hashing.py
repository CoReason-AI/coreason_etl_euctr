# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import hashlib


def compute_content_hash(content: str) -> str:
    """
    Compute the SHA-256 hash of the content string.

    Args:
        content: The input string content (e.g., HTML).

    Returns:
        The hex digest of the SHA-256 hash.

    Raises:
        ValueError: If content is None.
    """
    if content is None:
        raise ValueError("Content cannot be None")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
