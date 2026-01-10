# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import IO

import pytest

from coreason_etl_euctr.loader import BaseLoader


def test_cannot_instantiate_abstract_class() -> None:
    """Test that BaseLoader cannot be instantiated directly."""
    with pytest.raises(TypeError) as excinfo:
        BaseLoader()  # type: ignore[abstract]
    assert "Can't instantiate abstract class BaseLoader" in str(excinfo.value)


def test_concrete_class_missing_methods() -> None:
    """Test that a concrete subclass fails if it doesn't implement all abstract methods."""

    class IncompleteLoader(BaseLoader):
        def connect(self) -> None:
            pass

    with pytest.raises(TypeError) as excinfo:
        IncompleteLoader()  # type: ignore[abstract]
    assert "Can't instantiate abstract class IncompleteLoader" in str(excinfo.value)


def test_valid_concrete_implementation() -> None:
    """Test that a valid concrete subclass can be instantiated."""

    class MockLoader(BaseLoader):
        def connect(self) -> None:
            pass

        def close(self) -> None:
            pass

        def prepare_schema(self) -> None:
            pass

        def bulk_load_stream(self, data_stream: IO[str], target_table: str) -> None:
            pass

        def upsert_stream(self, data_stream: IO[str], target_table: str, conflict_keys: list[str]) -> None:
            pass

        def truncate_tables(self, table_names: list[str]) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    loader = MockLoader()
    assert isinstance(loader, BaseLoader)
    # Ensure methods can be called
    loader.connect()
    loader.close()
    loader.prepare_schema()
    loader.commit()
    loader.rollback()
