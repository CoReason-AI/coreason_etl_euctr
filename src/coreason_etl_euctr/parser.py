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
AGENT INSTRUCTION: This module defines the EpistemicParserTask to parse EU CTR
HTML documents into a universal JSON dictionary structure (Silver Layer).
It enforces Anchor-Based Extraction mapping HTML section codes to their values
and implements Hierarchical Block Grouping for nested repeating sections.
"""

import re
from typing import Any, cast

from bs4 import BeautifulSoup


class EpistemicParserTask:
    """
    Manages the parsing of raw EU CTR HTML protocols into structured JSON format.
    """

    def __init__(self) -> None:
        """
        Initializes the Parser task.
        """
        self._block_pattern = re.compile(r"^(.+?):\s*(\d+)$")

    def parse_html(self, html_content: str) -> dict[str, Any]:
        """
        Parses the raw HTML into a comprehensive, normalized JSON dictionary.

        Args:
            html_content: The HTML string to parse.

        Returns:
            A dictionary containing the parsed data using Anchor-Based Extraction
            and Hierarchical Block Grouping.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        parsed_data: dict[str, Any] = {}

        current_block_name: str | None = None
        current_block_index: int | None = None

        # Iterate over all table rows
        for tr in soup.find_all("tr"):
            # Check for a group header row (e.g. <td colspan="3" class="cellBlue">D.IMP: 1</td>)
            # Sometimes colspan might not be 3, just check class and content format
            td_header = tr.find("td", class_="cellBlue")
            if td_header:
                header_text = td_header.get_text(strip=True)
                match = self._block_pattern.match(header_text)
                if match:
                    block_name = match.group(1).strip()
                    block_index = int(match.group(2))

                    if block_name not in parsed_data:
                        parsed_data[block_name] = []

                    # Ensure the list is large enough for the current index
                    # E.g., if index is 2, length must be at least 2
                    while len(parsed_data[block_name]) < block_index:
                        parsed_data[block_name].append({})

                    current_block_name = block_name
                    # 1-based index in HTML, 0-based in Python list
                    current_block_index = block_index - 1
                    continue
                # Some cellBlue might just be regular headers, reset grouping
                current_block_name = None
                current_block_index = None
                continue

            # Find data rows
            td_first = tr.find("td", class_="first")
            td_third = tr.find("td", class_="third")

            if td_first and td_third:
                key = td_first.get_text(strip=True)
                # Use separator space to handle multi-line content or inner tags correctly
                val = td_third.get_text(separator=" ", strip=True)

                if not key:
                    continue

                if current_block_name is not None and current_block_index is not None:
                    # Cast for type checker to know we're working with a list of dicts
                    block_list = cast("list[dict[str, Any]]", parsed_data[current_block_name])
                    block_list[current_block_index][key] = val
                else:
                    parsed_data[key] = val

        return parsed_data
