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
Tests for the EpistemicGoldLoaderTask and EpistemicGoldManifest.
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from coreason_etl_euctr.gold_loader import EpistemicGoldLoaderTask, EpistemicGoldManifest


@pytest.fixture
def valid_gold_data() -> dict[str, str | bool | None]:
    """Provides a valid dictionary conforming to EpistemicGoldManifest."""
    return {
        "coreason_id": "123e4567-e89b-12d3-a456-426614174000",
        "source_id": "2023-001234-56",
        "A.3": "Test Trial Title",
        "B.1.1": "Test Sponsor",
        "trial_status_coalesced": "Completed",
        "E.1.1.2": "Therapeutic Area X",
        "E.2.1": "Main Objective",
        "E.2.2": "Secondary Objective",
        "E.3": "Inclusion Criteria",
        "E.4": "Exclusion Criteria",
        "E.5.1": "Primary Endpoint",
        "E.5.2": "Secondary Endpoint",
        "E.7.1": True,
        "E.7.2": False,
        "E.7.3": None,
        "E.7.4": None,
        "products": '[{"D.2.1.1.1": "Trade Name"}]',
    }


def test_manifest_validation_success(valid_gold_data: dict[str, str | bool | None]) -> None:
    """Test that a valid dictionary successfully parses into the manifest."""
    manifest = EpistemicGoldManifest.model_validate(valid_gold_data)
    assert manifest.coreason_id == "123e4567-e89b-12d3-a456-426614174000"
    assert manifest.source_id == "2023-001234-56"
    assert manifest.A_3 == "Test Trial Title"
    assert manifest.E_7_1 is True

    # Test serialization by alias
    dumped = manifest.model_dump(by_alias=True)
    assert dumped["A.3"] == "Test Trial Title"
    assert "A_3" not in dumped


def test_manifest_validation_failure_missing_required() -> None:
    """Test that validation fails if required fields are missing."""
    invalid_data = {
        "A.3": "Test Trial Title",
    }
    with pytest.raises(ValueError, match="Field required"):
        EpistemicGoldManifest.model_validate(invalid_data)


def test_manifest_validation_failure_extra_fields(valid_gold_data: dict[str, str | bool | None]) -> None:
    """Test that validation fails if extra unexpected fields are present."""
    invalid_data = valid_gold_data.copy()
    invalid_data["unexpected_field"] = "bad"
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        EpistemicGoldManifest.model_validate(invalid_data)


class TestEpistemicGoldLoaderTask:
    """Test suite for the Gold Loader Task."""

    @pytest.fixture
    def gold_loader(self) -> EpistemicGoldLoaderTask:
        return EpistemicGoldLoaderTask(pipeline_name="test_pipeline", destination="duckdb", dataset_name="test_gold")

    @patch("coreason_etl_euctr.gold_loader.logger.warning")
    def test_validate_dataframe_success(
        self,
        mock_logger_warning: MagicMock,
        gold_loader: EpistemicGoldLoaderTask,
        valid_gold_data: dict[str, str | bool | None],
    ) -> None:
        """Test validation of a valid Polars DataFrame."""
        df = pl.DataFrame([valid_gold_data])
        gold_loader.validate_dataframe(df)
        mock_logger_warning.assert_not_called()

    @patch("coreason_etl_euctr.gold_loader.logger.warning")
    def test_validate_dataframe_partial_failure(
        self,
        mock_logger_warning: MagicMock,
        gold_loader: EpistemicGoldLoaderTask,
        valid_gold_data: dict[str, str | bool | None],
    ) -> None:
        """Test validation where one row is valid and another is invalid (missing required)."""
        invalid_data = {"A.3": "Invalid Row Missing IDs"}
        df = pl.DataFrame([valid_gold_data, invalid_data])

        gold_loader.validate_dataframe(df)

        # Logger warning should be called once for the invalid row
        assert mock_logger_warning.call_count == 1
        assert "Data validation failed for record None" in mock_logger_warning.call_args[0][0]

    def test_validate_dataframe_empty(self, gold_loader: EpistemicGoldLoaderTask) -> None:
        """Test validation of an empty DataFrame."""
        df = pl.DataFrame()
        # Should not raise or do anything
        gold_loader.validate_dataframe(df)

    @patch("dlt.pipeline")
    def test_load_gold_dataframe_success(
        self,
        mock_dlt_pipeline: MagicMock,
        gold_loader: EpistemicGoldLoaderTask,
        valid_gold_data: dict[str, str | bool | None],
    ) -> None:
        """Test successfully loading data via dlt."""
        mock_pipeline_instance = MagicMock()
        mock_load_info = MagicMock()
        mock_pipeline_instance.run.return_value = mock_load_info
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        df = pl.DataFrame([valid_gold_data])

        result = gold_loader.load_gold_dataframe(df, write_disposition="merge")

        assert result == mock_load_info
        mock_dlt_pipeline.assert_called_once_with(
            pipeline_name="test_pipeline",
            destination="duckdb",
            dataset_name="test_gold",
        )
        mock_pipeline_instance.run.assert_called_once()
        positional_args = mock_pipeline_instance.run.call_args[0]
        call_args = mock_pipeline_instance.run.call_args[1]

        assert call_args["table_name"] == "coreason_etl_euctr_gold_euctr_rag"
        assert call_args["write_disposition"] == "merge"
        assert call_args["primary_key"] == "coreason_id"
        assert positional_args[0] == df.to_dicts()

    @patch("dlt.pipeline")
    def test_load_gold_dataframe_empty_dataframe(
        self, mock_dlt_pipeline: MagicMock, gold_loader: EpistemicGoldLoaderTask
    ) -> None:
        """Test loading an empty DataFrame returns None and does not trigger dlt."""
        df = pl.DataFrame()
        result = gold_loader.load_gold_dataframe(df)

        assert result is None
        mock_dlt_pipeline.assert_not_called()

    @patch("dlt.pipeline")
    @patch("coreason_etl_euctr.gold_loader.logger.warning")
    def test_load_gold_dataframe_all_invalid(
        self, mock_logger_warning: MagicMock, mock_dlt_pipeline: MagicMock, gold_loader: EpistemicGoldLoaderTask
    ) -> None:
        """Test loading a DataFrame where all rows are invalid logs warnings but attempts dlt."""
        invalid_data = {"A.3": "Invalid"}
        df = pl.DataFrame([invalid_data])

        mock_pipeline_instance = MagicMock()
        mock_load_info = MagicMock()
        mock_pipeline_instance.run.return_value = mock_load_info
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        result = gold_loader.load_gold_dataframe(df)

        assert result == mock_load_info
        assert mock_logger_warning.call_count == 1
        mock_dlt_pipeline.assert_called_once()
        mock_pipeline_instance.run.assert_called_once()
        positional_args = mock_pipeline_instance.run.call_args[0]
        assert positional_args[0] == df.to_dicts()

    @patch("dlt.pipeline")
    def test_load_gold_dataframe_replace_mode(
        self,
        mock_dlt_pipeline: MagicMock,
        gold_loader: EpistemicGoldLoaderTask,
        valid_gold_data: dict[str, str | bool | None],
    ) -> None:
        """Test that write_disposition replace does not pass a primary_key."""
        mock_pipeline_instance = MagicMock()
        mock_pipeline_instance.run.return_value = MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        df = pl.DataFrame([valid_gold_data])

        gold_loader.load_gold_dataframe(df, write_disposition="replace")

        call_args = mock_pipeline_instance.run.call_args[1]
        assert call_args["write_disposition"] == "replace"
        assert "primary_key" not in call_args

    @patch("dlt.pipeline")
    def test_load_gold_dataframe_unexpected_type_error(
        self,
        mock_dlt_pipeline: MagicMock,
        gold_loader: EpistemicGoldLoaderTask,
        valid_gold_data: dict[str, str | bool | None],
    ) -> None:
        """Test that unexpected TypeErrors are reraised."""
        mock_pipeline_instance = MagicMock()
        mock_pipeline_instance.run.side_effect = TypeError("Some other type error")
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        df = pl.DataFrame([valid_gold_data])

        with pytest.raises(TypeError, match="Some other type error"):
            gold_loader.load_gold_dataframe(df)


# Use Hypothesis for property-based testing of edge case inputs
@settings(max_examples=50)  # type: ignore[misc]
@given(st.text(min_size=1), st.text(min_size=1), st.one_of(st.none(), st.text()), st.one_of(st.none(), st.booleans()))  # type: ignore[misc]
def test_manifest_property_based(coreason_id: str, source_id: str, a3_val: str | None, phase_val: bool | None) -> None:
    """Property-based test to ensure varying inputs are accepted when valid."""
    data = {
        "coreason_id": coreason_id,
        "source_id": source_id,
        "A.3": a3_val,
        "E.7.1": phase_val,
    }

    # Should not raise
    manifest = EpistemicGoldManifest.model_validate(data)
    assert manifest.coreason_id == coreason_id
    assert manifest.source_id == source_id
    assert a3_val == manifest.A_3
    assert phase_val == manifest.E_7_1
