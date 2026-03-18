# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

"""
AGENT INSTRUCTION: This module provides a deterministic hashing utility for dictionaries,
guaranteeing stable hash values for parsed Silver JSON structures.
"""

import hashlib
import json
from typing import Any


def generate_deterministic_hash(data: dict[str, Any]) -> str:
    """
    Generates a mathematically deterministic SHA-256 hash for a given dictionary.
    Keys are sorted to ensure traversal order does not affect the final hash.

    Args:
        data: The dictionary (Silver JSON payload) to be hashed.

    Returns:
        A hexadecimal string representation of the SHA-256 hash.
    """
    # Use sort_keys=True to ensure deterministic order of keys
    # Use separators=(',', ':') to eliminate whitespace variances
    serialized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
