# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from bs4 import BeautifulSoup

from coreason_etl_euctr.utils import extract_field_by_label


def test_extract_field_by_label_unicode_colon() -> None:
    """
    Test that extract_field_by_label handles unicode fullwidth colons (\uff1a).
    This ensures robustness against varied HTML sources.
    """
    # Case 1: Label with Fullwidth Colon in the text node
    html_wide = "<table><tr><td>Name of Sponsor\uff1a</td><td>Wide Sponsor</td></tr></table>"
    soup = BeautifulSoup(html_wide, "html.parser")
    val = extract_field_by_label(soup, "Name of Sponsor")
    assert val == "Wide Sponsor"

    # Case 2: Label followed by Fullwidth Colon (implicit match via regex prefix)
    html_wide_2 = "<table><tr><td>Name of Sponsor \uff1a</td><td>Wide Sponsor 2</td></tr></table>"
    soup_2 = BeautifulSoup(html_wide_2, "html.parser")
    val_2 = extract_field_by_label(soup_2, "Name of Sponsor")
    assert val_2 == "Wide Sponsor 2"
