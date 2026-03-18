# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from coreason_etl_euctr.utils.hashing import generate_deterministic_hash


def test_generate_deterministic_hash_same_content_different_order() -> None:
    data1 = {"a": 1, "b": 2, "c": 3}
    data2 = {"b": 2, "c": 3, "a": 1}

    hash1 = generate_deterministic_hash(data1)
    hash2 = generate_deterministic_hash(data2)

    assert hash1 == hash2


def test_generate_deterministic_hash_nested_dict() -> None:
    data1 = {"a": {"x": 10, "y": 20}, "b": [1, 2, 3]}
    data2 = {"b": [1, 2, 3], "a": {"y": 20, "x": 10}}

    hash1 = generate_deterministic_hash(data1)
    hash2 = generate_deterministic_hash(data2)

    assert hash1 == hash2


def test_generate_deterministic_hash_different_content() -> None:
    data1 = {"a": 1, "b": 2}
    data2 = {"a": 1, "b": 3}

    hash1 = generate_deterministic_hash(data1)
    hash2 = generate_deterministic_hash(data2)

    assert hash1 != hash2
