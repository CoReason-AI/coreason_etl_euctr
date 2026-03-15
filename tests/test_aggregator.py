# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import polars as pl

from coreason_etl_euctr.aggregator import EpistemicGoldAggregatorTask


def test_clean_text() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    data = {
        "A.2": ["2020-000000-00"],
        "E.3": ["  Text with   spaces &nbsp; and \xa0  tags <b>bold</b>  "],
        "A.3": [None],
    }
    df = pl.DataFrame(data)

    # E.3 and A.3 are passed for cleaning
    cleaned_df = aggregator.clean_text(df, ["E.3", "A.3"])

    assert cleaned_df["E.3"][0] == "Text with spaces and tags bold"
    assert cleaned_df["A.2"][0] == "2020-000000-00"
    assert cleaned_df["A.3"][0] is None


def test_aggregate_empty_list() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    df = aggregator.aggregate([])
    assert df.is_empty()


def test_aggregate_projection() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    silver_data = [
        {
            "A.2": "2020-000000-00",
            "A.3": "Test Title",
            "E.3": "   Criteria   <br/>  1   ",
            "Z.9": "Should be ignored",  # Not in core_fields
        },
        {
            "A.2": "2021-111111-11",
            "B.1.1": "Test Sponsor",
        },
    ]

    df = aggregator.aggregate(silver_data)

    expected_columns = ["A.2", "A.3", "B.1.1", "E.1.1.2", "E.2.1", "E.2.2", "E.3", "E.4", "E.5.1", "E.5.2"]

    assert list(df.columns) == expected_columns
    assert len(df) == 2

    # Check cleaning and projection on first row
    assert df["A.2"][0] == "2020-000000-00"
    assert df["A.3"][0] == "Test Title"
    assert df["B.1.1"][0] is None
    assert df["E.3"][0] == "Criteria 1"

    # Check second row missing values
    assert df["A.2"][1] == "2021-111111-11"
    assert df["B.1.1"][1] == "Test Sponsor"
    assert df["A.3"][1] is None
