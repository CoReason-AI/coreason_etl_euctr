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
from pytest_mock import MockerFixture

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
        "coreason_id",
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
        "trial_status_coalesced",
    ]

    assert list(df.columns) == expected_columns
    assert len(df) == 2

    # Because group_by might reorder based on sorting (though maintain_order=True),
    # let's locate rows by source_id
    row1 = df.filter(pl.col("source_id") == "2020-000000-00").row(0, named=True)
    row2 = df.filter(pl.col("source_id") == "2021-111111-11").row(0, named=True)

    # Check cleaning and projection on first row
    assert row1["source_id"] == "2020-000000-00"
    assert row1["A.3"] == "Test Title"
    assert row1["B.1.1"] is None
    assert row1["E.3"] == "Criteria 1"

    # Check second row missing values
    assert row2["source_id"] == "2021-111111-11"
    assert row2["B.1.1"] == "Test Sponsor"
    assert row2["A.3"] is None


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

    # After grouping, we should have 3 rows instead of 4, since rows with same A.2 group together
    assert len(df) == 3

    # get the row for expected_uuid_1
    row1 = df.filter(pl.col("coreason_id") == expected_uuid_1)
    assert len(row1) == 1
    assert row1["source_id"][0] == eudract_id_1

    row2 = df.filter(pl.col("coreason_id") == expected_uuid_2)
    assert len(row2) == 1
    assert row2["source_id"][0] == eudract_id_2

    row3 = df.filter(pl.col("coreason_id").is_null())
    assert len(row3) == 1
    assert row3["source_id"][0] is None


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

    row1 = df.filter(pl.col("source_id") == "2020-000000-00").row(0, named=True)
    row2 = df.filter(pl.col("source_id") == "2021-111111-11").row(0, named=True)
    row3 = df.filter(pl.col("source_id") == "2022-222222-22").row(0, named=True)

    assert row1["E.7.1"] is True
    assert row1["E.7.2"] is False
    assert row1["E.7.3"] is True
    assert row1["E.7.4"] is False

    assert row2["E.7.1"] is True
    assert row2["E.7.2"] is False
    assert row2["E.7.3"] is True
    assert row2["E.7.4"] is False

    assert row3["E.7.1"] is None
    assert row3["E.7.2"] is None
    assert row3["E.7.3"] is None
    assert row3["E.7.4"] is None


@given(  # type: ignore[misc] # codespell:ignore
    e71=st.sampled_from(["yes", "Yes", "true", "TRUE", "1"]),
    e72=st.sampled_from(["no", "NO", "false", "False", "0"]),
    e73=st.text(alphabet=st.characters(blacklist_categories=("Nd", "Cs")), min_size=1).filter(  # codespell:ignore
        lambda x: not re.search(r"\b(yes|true|1|no|false|0)\b", x.lower())
    ),
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

    row1 = df.filter(pl.col("source_id") == "111").row(0, named=True)
    row2 = df.filter(pl.col("source_id") == "222").row(0, named=True)
    row3 = df.filter(pl.col("source_id") == "333").row(0, named=True)

    prods_111 = json.loads(row1["products"])
    assert len(prods_111) == 2
    assert prods_111[0] == {"D.2.1.1.1": "TradeA", "D.3.1": "ProductA", "D.3.8": "SubstanceA", "D.3.4": "FormA"}
    assert prods_111[1] == {"D.2.1.1.1": "TradeB", "D.3.1": None, "D.3.8": None, "D.3.4": "FormB"}

    assert row2["products"] is None
    assert row3["products"] is None


def test_aggregate_trial_status_coalescing() -> None:
    aggregator = EpistemicGoldAggregatorTask()
    silver_data = [
        {"A.2": "123", "National trial status": "Ongoing", "A.3": "Title1"},
        {"A.2": "123", "National trial status": "Completed"},
        {"A.2": "123", "National trial status": "Ongoing"},  # Check unique
        {"A.2": "456", "A.3": "Title2"},
    ]

    df = aggregator.aggregate(silver_data)

    assert len(df) == 2

    row1 = df.filter(pl.col("source_id") == "123").row(0, named=True)
    assert row1["trial_status_coalesced"] == "Completed, Ongoing"
    assert row1["A.3"] == "Title1"

    row2 = df.filter(pl.col("source_id") == "456").row(0, named=True)
    assert row2["trial_status_coalesced"] is None


def test_aggregate_logs_warning_for_missing_fields(mocker: MockerFixture) -> None:
    mock_logger_warning = mocker.patch("coreason_etl_euctr.aggregator.logger.warning")
    aggregator = EpistemicGoldAggregatorTask()

    silver_data = [
        {"A.2": "2020-000000-00", "A.3": "Test", "E.3": "Criteria"},  # Missing E.4
        {"A.2": "2021-111111-11", "E.4": "Exclusion"},  # Missing E.3
    ]

    aggregator.aggregate(silver_data)

    mock_logger_warning.assert_any_call("Data mapping quality issue: Section E.4 missing for 2020-000000-00")
    mock_logger_warning.assert_any_call("Data mapping quality issue: Section E.3 missing for 2021-111111-11")
    assert mock_logger_warning.call_count == 2
