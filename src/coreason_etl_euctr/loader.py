# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from abc import ABC, abstractmethod
from typing import IO, List


class BaseLoader(ABC):
    """
    Abstract Base Class for Database Loaders (Adapter Pattern).
    """

    @abstractmethod
    def connect(self) -> None:  # pragma: no cover
        """Establishes connection to the target database."""
        pass

    @abstractmethod
    def prepare_schema(self) -> None:  # pragma: no cover
        """Creates necessary tables (Bronze/Silver) if they do not exist."""
        pass

    @abstractmethod
    def truncate_tables(self) -> None:  # pragma: no cover
        """Truncates the Silver layer tables for a full load."""
        pass

    @abstractmethod
    def bulk_load_stream(self, table_name: str, data_stream: IO[str], columns: List[str]) -> None:  # pragma: no cover
        """
        Loads data from a CSV-like stream into the target table.
        Must use native bulk loading capabilities (e.g. COPY).
        """
        pass

    @abstractmethod
    def upsert_stream(
        self, table_name: str, data_stream: IO[str], columns: List[str], conflict_keys: List[str]
    ) -> None:  # pragma: no cover
        """
        Loads data with upsert logic (Insert on Conflict Update).
        """
        pass

    @abstractmethod
    def close(self) -> None:  # pragma: no cover
        """Closes the connection."""
        pass
