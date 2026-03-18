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
AGENT INSTRUCTION: This module provides state management for CDC (Change Data Capture)
and Idempotency (Document Hashes) via a local JSON file.
"""

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from coreason_etl_euctr.utils.logger import logger


class EpistemicStateManifest(BaseModel):
    """
    Mathematical boundary contract representing the state of the ETL pipeline.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    last_run_timestamp: str | None = Field(None, description="The ISO 8601 timestamp of the last successful run.")
    document_hashes: dict[str, str] = Field(
        default_factory=dict, description="A mapping of EudraCT Numbers to their latest deterministic SHA-256 hash."
    )


class EpistemicStateManagerTask:
    """
    Manages the persistence and retrieval of the ETL pipeline state.
    """

    def __init__(self, state_file_path: str | None = None) -> None:
        """
        Initializes the state manager with a path to the state file.
        """
        from coreason_etl_euctr.utils.config import settings

        self.state_file_path = Path(state_file_path or settings.state_file_path)
        self._state: EpistemicStateManifest = self._load_state()

    def _load_state(self) -> EpistemicStateManifest:
        """
        Loads the state from the file. If the file doesn't exist or is invalid,
        returns an empty state.
        """
        if not self.state_file_path.exists():
            return EpistemicStateManifest()

        try:
            with open(self.state_file_path, encoding="utf-8") as f:
                data = json.load(f)
                return EpistemicStateManifest.model_validate(data)
        except Exception as e:
            logger.error(f"Failed to load state from {self.state_file_path}: {e}. Proceeding with empty state.")
            return EpistemicStateManifest()

    def _save_state(self) -> None:
        """
        Persists the current state to the file.
        """
        try:
            # Ensure parent directory exists
            self.state_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file_path, "w", encoding="utf-8") as f:
                json.dump(self._state.model_dump(), f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file_path}: {e}")
            raise e

    @property
    def last_run_timestamp(self) -> str | None:
        return self._state.last_run_timestamp

    @last_run_timestamp.setter
    def last_run_timestamp(self, value: str | None) -> None:
        self._state.last_run_timestamp = value
        self._save_state()

    def get_hash(self, eudract_id: str) -> str | None:
        """
        Retrieves the last known hash for a given EudraCT Number.
        """
        return self._state.document_hashes.get(eudract_id)

    def update_hash(self, eudract_id: str, new_hash: str) -> None:
        """
        Updates the hash for a given EudraCT Number and saves the state.
        """
        self._state.document_hashes[eudract_id] = new_hash
        self._save_state()
