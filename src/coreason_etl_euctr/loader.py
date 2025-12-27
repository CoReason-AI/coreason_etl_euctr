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
    Abstract Base Class for database loaders.
    Implements the Adapter Pattern to ensure database agnosticism.
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the target database.
        Credentials should be retrieved from environment variables within implementation.
        """
        pass  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        """
        Close the database connection.
        """
        pass  # pragma: no cover

    @abstractmethod
    def prepare_schema(self) -> None:
        """
        Create necessary tables (eu_trials, eu_trial_drugs, eu_trial_conditions)
        if they do not exist.
        """
        pass  # pragma: no cover

    @abstractmethod
    def bulk_load_stream(self, data_stream: IO[str], target_table: str) -> None:
        """
        Execute native bulk load operation from a CSV data stream.

        Args:
            data_stream: A file-like object containing CSV data.
            target_table: The name of the table to load data into.
        """
        pass  # pragma: no cover

    @abstractmethod
    def upsert_stream(self, data_stream: IO[str], target_table: str, conflict_keys: List[str]) -> None:
        """
        Execute an upsert (merge) operation from a CSV data stream.
        This typically involves loading to a temporary staging table and then
        performing an INSERT ... ON CONFLICT ... UPDATE into the target table.

        Args:
            data_stream: A file-like object containing CSV data.
            target_table: The name of the target table.
            conflict_keys: List of column names to use for conflict resolution (Primary Key).
        """
        pass  # pragma: no cover

    @abstractmethod
    def truncate_tables(self, table_names: List[str]) -> None:
        """
        Truncate the specified tables.

        Args:
            table_names: A list of table names to truncate.
        """
        pass  # pragma: no cover

    @abstractmethod
    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        pass  # pragma: no cover

    @abstractmethod
    def rollback(self) -> None:
        """
        Rollback the current transaction.
        """
        pass  # pragma: no cover
