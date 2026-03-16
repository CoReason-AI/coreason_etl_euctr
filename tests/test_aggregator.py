# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import re

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_euctr.aggregator import EpistemicGoldAggregatorTask


def test_clean_text() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    data = {
        "source_id": ["2020-000000-00"],
        "E.3": ["  Text with   spaces &nbsp; and \xa0  tags <b>bold</b>  "],
        "A.3": [None],
    }
    df = pl.DataFrame(data)

    # E.3 and A.3 are passed for cleaning
    cleaned_df = aggregator.clean_text(df, ["E.3", "A.3"])

    assert cleaned_df["E.3"][0] == "Text with spaces and tags bold"
    assert cleaned_df["source_id"][0] == "2020-000000-00"
    assert cleaned_df["A.3"][0] is None


def test_aggregate_empty_list() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    df = aggregator.aggregate([])
    assert df.is_empty()


def test_aggregate_missing_a2_fallback() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    silver_data = [{"A.3": "Title Only, no A.2 field at all in dataframe"}]
    df = aggregator.aggregate(silver_data)

    assert "source_id" in df.columns
    assert "coreason_id" in df.columns
    assert df["source_id"][0] is None
    assert df["coreason_id"][0] is None


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

    expected_columns = [
        "source_id",
        "A.3",
        "B.1.1",
        "E.1.1.2",
        "E.2.1",
        "E.2.2",
        "E.3",
        "E.4",
        "E.5.1",
        "E.5.2",
        "products",
        "E.7.1",
        "E.7.2",
        "E.7.3",
        "E.7.4",
        "coreason_id",
    ]

    assert list(df.columns) == expected_columns
    assert len(df) == 2

    # Check cleaning and projection on first row
    assert df["source_id"][0] == "2020-000000-00"
    assert df["A.3"][0] == "Test Title"
    assert df["B.1.1"][0] is None
    assert df["E.3"][0] == "Criteria 1"

    # Check second row missing values
    assert df["source_id"][1] == "2021-111111-11"
    assert df["B.1.1"][1] == "Test Sponsor"
    assert df["A.3"][1] is None


def test_aggregate_identity_resolution() -> None:
    import uuid

    from coreason_etl_euctr.aggregator import NAMESPACE_EUCTR

    aggregator = EpistemicGoldAggregatorTask()
    eudract_id_1 = "2020-000000-00"
    eudract_id_2 = "2021-111111-11"

    silver_data = [
        {"A.2": eudract_id_1},
        {"A.2": eudract_id_1},  # duplicate to check deterministic uuid
        {"A.2": eudract_id_2},
        {"A.3": "No A.2 present"},  # missing A.2
    ]

    df = aggregator.aggregate(silver_data)

    assert "source_id" in df.columns
    assert "coreason_id" in df.columns

    expected_uuid_1 = str(uuid.uuid5(NAMESPACE_EUCTR, eudract_id_1))
    expected_uuid_2 = str(uuid.uuid5(NAMESPACE_EUCTR, eudract_id_2))

    assert df["coreason_id"][0] == expected_uuid_1
    assert df["coreason_id"][1] == expected_uuid_1  # deterministic check
    assert df["coreason_id"][2] == expected_uuid_2
    assert df["coreason_id"][3] is None  # missing source_id should result in null coreason_id


def test_aggregate_trial_phases() -> None:
    from typing import Any

    aggregator = EpistemicGoldAggregatorTask()
    silver_data: list[dict[str, Any]] = [
        {
            "A.2": "2020-000000-00",
            "E.7.1": "Yes",
            "E.7.2": "No",
            "E.7.3": "yes ",
            "E.7.4": " NO ",
        },
        {
            "A.2": "2021-111111-11",
            "E.7.1": "true",
            "E.7.2": "False",
            "E.7.3": "1",
            "E.7.4": "0",
        },
        {
            "A.2": "2022-222222-22",
            "E.7.1": "unknown",
            "E.7.2": None,
        },
    ]

    df = aggregator.aggregate(silver_data)

    assert df["E.7.1"][0] is True
    assert df["E.7.2"][0] is False
    assert df["E.7.3"][0] is True
    assert df["E.7.4"][0] is False

    assert df["E.7.1"][1] is True
    assert df["E.7.2"][1] is False
    assert df["E.7.3"][1] is True
    assert df["E.7.4"][1] is False

    assert df["E.7.1"][2] is None
    assert df["E.7.2"][2] is None
    assert df["E.7.3"][2] is None
    assert df["E.7.4"][2] is None


@given(  # type: ignore[misc]
    e71=st.sampled_from(["yes", "Yes", "true", "TRUE", "1"]),
    e72=st.sampled_from(["no", "NO", "false", "False", "0"]),
    e73=st.text().filter(lambda x: not re.search(r"\b(yes|true|1|no|false|0)\b", x.lower())),
    e74=st.none(),
)
def test_aggregate_trial_phases_hypothesis(e71: str, e72: str, e73: str, e74: str | None) -> None:
    aggregator = EpistemicGoldAggregatorTask()
    silver_data = [
        {
            "A.2": "2020-000000-00",
            "E.7.1": e71,
            "E.7.2": e72,
            "E.7.3": e73,
            "E.7.4": e74,
        }
    ]

    df = aggregator.aggregate(silver_data)

    assert df["E.7.1"][0] is True
    assert df["E.7.2"][0] is False
    assert df["E.7.3"][0] is None
    assert df["E.7.4"][0] is None


def test_aggregate_imp_flattening() -> None:
    import json
    from typing import Any

    aggregator = EpistemicGoldAggregatorTask()
    silver_data: list[dict[str, Any]] = [
        {
            "A.2": "111",
            "D.IMP": [
                {"D.2.1.1.1": "TradeA", "D.3.1": "ProductA", "D.3.8": "SubstanceA", "D.3.4": "FormA"},
                {"D.2.1.1.1": "TradeB", "D.3.4": "FormB"},
                {"Other": "Ignored"},
            ],
        },
        {"A.2": "222", "D.IMP": []},
        {"A.2": "333"},
    ]

    df = aggregator.aggregate(silver_data)

    assert len(df) == 3

    prods_111 = json.loads(df["products"][0])
    assert len(prods_111) == 2
    assert prods_111[0] == {"D.2.1.1.1": "TradeA", "D.3.1": "ProductA", "D.3.8": "SubstanceA", "D.3.4": "FormA"}
    assert prods_111[1] == {"D.2.1.1.1": "TradeB", "D.3.1": None, "D.3.8": None, "D.3.4": "FormB"}

    assert df["products"][1] is None
    assert df["products"][2] is None
