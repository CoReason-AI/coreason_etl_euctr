# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import re
from datetime import date, datetime
from typing import Optional, Union

from bs4 import BeautifulSoup, Tag


def clean_text(text: str) -> str:
    """
    Remove excessive whitespace and non-breaking spaces from text.

    Args:
        text: The input string.

    Returns:
        Cleaned string.
    """
    if not text:
        return ""
    # Replace non-breaking space
    text = text.replace("\xa0", " ")
    # Collapse whitespace
    text = " ".join(text.split())
    return text


def extract_field_by_label(soup: Union[BeautifulSoup, Tag], label_text: str) -> Optional[str]:
    """
    Find a field value by its label.
    Handles the messy table structure of EU CTR.

    Args:
        soup: The BeautifulSoup object or Tag to search within.
        label_text: The text of the label to search for (case-insensitive).

    Returns:
        The extracted and cleaned value, or None if not found.
    """
    # Find the label. Use a regex to be flexible with colons and whitespace
    label_pattern = re.compile(rf"{re.escape(label_text)}\s*:?", re.IGNORECASE)
    target = soup.find(string=label_pattern)

    if not target:
        return None

    # Logic:
    # 1. Check parent's siblings (td -> td)
    # 2. Check parent's parent's siblings (tr -> tr, if label is in a th or b)

    parent = target.parent
    if not parent:
        return None

    # Try next sibling of the parent element
    next_sibling = parent.find_next_sibling()

    # If the label is inside a <b> or <span> inside a <td>, we might need to go up one level
    if next_sibling is None and parent.name in ["b", "span", "strong", "font"]:
        parent = parent.parent
        if parent:
            next_sibling = parent.find_next_sibling()

    if next_sibling:
        text = next_sibling.get_text(strip=True)
        if text:
            return clean_text(text)

    return None


def parse_flexible_date(date_str: Optional[str]) -> Optional[date]:
    """
    Attempt to parse a date string in various formats.

    Args:
        date_str: The date string to parse.

    Returns:
        The parsed date object, or None if parsing failed.
    """
    if not date_str:
        return None

    # Formats: YYYY-MM-DD or DD/MM/YYYY
    # EU CTR usually uses YYYY-MM-DD but let's be robust
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        pass

    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        pass

    # Try with dots (German style) DD.MM.YYYY
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        pass

    # Requirement R.4.4.3: Robust Date Normalization
    # Raise ValueError for malformed dates to allow row rejection
    raise ValueError(f"Could not parse date: {date_str}")
