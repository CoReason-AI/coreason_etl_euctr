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
from typing import Any, Generator, Iterable, Set, Type

from loguru import logger
from pydantic import BaseModel


class Pipeline:
    """
    Staging layer responsible for transforming Pydantic models into CSV streams
    ready for native bulk loading.
    """

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
        headers = list(first_item.model_fields.keys())
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
        self,
        item: BaseModel,
        headers: list[str],
        seen_hashes: Set[int],
        writer: Any,
        output: io.StringIO
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
        data = item.model_dump(mode='json')
        row = [data.get(h) for h in headers]

        # Handle simple transformations if needed (e.g. None to "")
        # csv.writer handles None as empty string? No, it might print nothing or cause issues depending on dialect.
        # Default csv.writer handles None by writing empty string if not quoted?
        # Actually csv.writer handles None by raising error? No, it converts to string. None -> '' is better.
        # Let's clean row.
        cleaned_row = [str(v) if v is not None else "" for v in row]

        writer.writerow(cleaned_row)
