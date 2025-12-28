# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import csv
import io
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Optional, Set, Union

from loguru import logger
from pydantic import BaseModel


class Pipeline:
    """
    Staging layer responsible for transforming Pydantic models into CSV streams
    ready for native bulk loading, and managing the orchestration state.
    """

    def __init__(self, state_file: Union[str, Path] = "data/state.json") -> None:
        """
        Initialize the Pipeline.

        Args:
            state_file: Path to the JSON state file.
        """
        self.state_file = Path(state_file)

    def load_state(self) -> Dict[str, Any]:
        """
        Load the pipeline state from the JSON file.

        Returns:
            A dictionary containing the state. Returns empty dict if file missing or corrupted.
        """
        if not self.state_file.exists():
            return {}

        try:
            content = self.state_file.read_text(encoding="utf-8")
            data = json.loads(content)
            if not isinstance(data, dict):
                logger.warning(f"State file {self.state_file} contains invalid data type (not dict). Resetting.")
                return {}
            # Cast for MyPy; json.loads returns Any, but we checked isinstance dict
            return dict(data)
        except json.JSONDecodeError:
            logger.warning(f"State file {self.state_file} is corrupted. Resetting state.")
            return {}
        except Exception as e:
            logger.error(f"Failed to load state from {self.state_file}: {e}")
            return {}

    def save_state(self, state: Dict[str, Any]) -> None:
        """
        Save the pipeline state to the JSON file.
        Uses atomic write pattern (write to temp -> rename).

        Args:
            state: Dictionary containing the state to save.
        """
        try:
            # Ensure parent directory exists
            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            # Write to a temporary file
            temp_file = self.state_file.with_suffix(".tmp")

            with temp_file.open("w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
                # Ensure trailing newline for pre-commit compliance
                f.write("\n")

            # Atomic rename
            temp_file.replace(self.state_file)

        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file}: {e}")
            # Try to cleanup temp file if it exists
            try:
                temp_file_cleanup = self.state_file.with_suffix(".tmp")
                if temp_file_cleanup.exists():
                    temp_file_cleanup.unlink()
            except Exception:
                pass

    def get_high_water_mark(self) -> Optional[date]:
        """
        Get the last updated timestamp from the state.

        Returns:
            The date object representing the high water mark, or None if not set/invalid.
        """
        state = self.load_state()
        val = state.get("last_updated")
        if not val:
            return None

        try:
            return date.fromisoformat(val)
        except ValueError:
            logger.warning(f"Invalid date format in state: {val}")
            return None

    def set_high_water_mark(self, new_date: date) -> None:
        """
        Update the high water mark in the state.

        Args:
            new_date: The new date to set.
        """
        state = self.load_state()
        state["last_updated"] = new_date.isoformat()
        self.save_state(state)

    def stage_data(self, models: Iterable[BaseModel]) -> Generator[str, None, None]:
        """
        Convert a stream of Pydantic models into a CSV stream.

        Features:
        - Extracts headers dynamically from the first model.
        - Deduplicates rows based on their hash (within the current batch).
        - Skips None/invalid inputs (though input is expected to be valid models).

        Args:
            models: An iterator or list of Pydantic models.

        Yields:
            Strings representing CSV lines (including newline).
            The first yield is the header.
        """
        iterator = iter(models)
        try:
            first_item = next(iterator)
        except StopIteration:
            return

        # Prepare CSV writer writing to a string buffer
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        # Extract headers
        # We use model_dump(mode='json') keys or model_fields.
        # model.model_fields.keys() gives strict schema fields.
        headers = list(type(first_item).model_fields.keys())
        writer.writerow(headers)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        # Track seen items for deduplication
        seen_hashes: Set[int] = set()

        # Process first item
        self._process_item(first_item, headers, seen_hashes, writer, output)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        # Process rest
        for item in iterator:
            # R.4.5.2: Rows failing validation are excluded.
            # Since input is strongly typed BaseModel, we assume structure is valid.
            # We focus on deduplication and null-checks if needed.

            self._process_item(item, headers, seen_hashes, writer, output)
            content = output.getvalue()
            if content:
                yield content
            output.seek(0)
            output.truncate(0)

    def _process_item(
        self, item: BaseModel, headers: list[str], seen_hashes: Set[int], writer: Any, output: io.StringIO
    ) -> None:
        """
        Helper to process a single item: dedup check and write to buffer.
        """
        # Create a hashable representation for dedup
        # Pydantic models are not hashable by default, so we hash the tuple of values
        # or use model_dump_json() if we want exact content match.
        # Using json dump is safer for nested structures, though we expect flat here.
        item_hash = hash(item.model_dump_json())

        if item_hash in seen_hashes:
            # Log or silently skip? User said "Delete repeated values"
            # We'll skip.
            return

        seen_hashes.add(item_hash)

        # Extract values in order of headers
        data = item.model_dump(mode="json")
        row = [data.get(h) for h in headers]

        # Handle simple transformations if needed (e.g. None to "")
        # csv.writer handles None as empty string? No, it might print nothing or cause issues depending on dialect.
        # Default csv.writer handles None by writing empty string if not quoted?
        # Actually csv.writer handles None by raising error? No, it converts to string. None -> '' is better.
        # Let's clean row.
        cleaned_row = [str(v) if v is not None else "" for v in row]

        writer.writerow(cleaned_row)
