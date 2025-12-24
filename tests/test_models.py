# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date, datetime

import pytest
from coreason_etl_euctr.models import EuTrial, EuTrialCondition, EuTrialDrug
from pydantic import ValidationError


def test_eu_trial_valid() -> None:
    """Test creating a valid EuTrial object."""
    trial = EuTrial(
        eudract_number="2023-123456-78",
        sponsor_name="Acme Corp",
        trial_title="A Test Trial",
        start_date=date(2023, 1, 1),
        trial_status="Ongoing",
        url_source="https://example.com",
    )
    assert trial.eudract_number == "2023-123456-78"
    assert trial.sponsor_name == "Acme Corp"
    assert trial.start_date == date(2023, 1, 1)
    assert isinstance(trial.last_updated, datetime)


def test_eu_trial_missing_required() -> None:
    """Test validation error for missing required fields in EuTrial."""
    with pytest.raises(ValidationError) as excinfo:
        EuTrial(sponsor_name="Acme Corp")  # type: ignore[call-arg]

    assert "eudract_number" in str(excinfo.value)
    assert "url_source" in str(excinfo.value)


def test_eu_trial_defaults() -> None:
    """Test default values for EuTrial."""
    trial = EuTrial(eudract_number="123", url_source="http://test.com")  # type: ignore[call-arg]
    assert trial.sponsor_name is None
    assert trial.start_date is None
    assert isinstance(trial.last_updated, datetime)


def test_eu_trial_drug_valid() -> None:
    """Test creating a valid EuTrialDrug object."""
    drug = EuTrialDrug(
        eudract_number="2023-123456-78",
        drug_name="WonderDrug",
        active_ingredient="Wondrium",
        cas_number="123-45-6",
        pharmaceutical_form="Tablet",
    )
    assert drug.eudract_number == "2023-123456-78"
    assert drug.drug_name == "WonderDrug"


def test_eu_trial_drug_missing_fk() -> None:
    """Test validation error for missing foreign key in EuTrialDrug."""
    with pytest.raises(ValidationError):
        EuTrialDrug(drug_name="Test")  # type: ignore[call-arg]


def test_eu_trial_condition_valid() -> None:
    """Test creating a valid EuTrialCondition object."""
    condition = EuTrialCondition(
        eudract_number="2023-123456-78",
        condition_name="Headache",
        meddra_code="10019211",
    )
    assert condition.eudract_number == "2023-123456-78"
    assert condition.condition_name == "Headache"


def test_eu_trial_condition_missing_fk() -> None:
    """Test validation error for missing foreign key in EuTrialCondition."""
    with pytest.raises(ValidationError):
        EuTrialCondition(condition_name="Flu")  # type: ignore[call-arg]


# --- Complex and Edge Case Tests ---


def test_eu_trial_date_coercion() -> None:
    """Test that string dates are correctly coerced to date objects."""
    trial = EuTrial(
        eudract_number="123",
        url_source="http://test.com",
        start_date="2023-01-01",  # type: ignore[arg-type]
    )
    assert trial.start_date == date(2023, 1, 1)


def test_eu_trial_invalid_date() -> None:
    """Test that invalid date strings raise ValidationError."""
    with pytest.raises(ValidationError) as excinfo:
        EuTrial(
            eudract_number="123",
            url_source="http://test.com",
            start_date="not-a-date",  # type: ignore[arg-type]
        )
    assert "start_date" in str(excinfo.value)


def test_eu_trial_type_coercion_failure() -> None:
    """Test that int to str coercion does not happen by default in Pydantic V2."""
    with pytest.raises(ValidationError) as excinfo:
        EuTrial(
            eudract_number=12345,  # type: ignore[arg-type]
            url_source="http://test.com",
        )
    assert "eudract_number" in str(excinfo.value)
    assert "Input should be a valid string" in str(excinfo.value)


def test_eu_trial_unicode_handling() -> None:
    """Test that models handle Unicode characters correctly."""
    unicode_str = "Sponsor with Üñíçødé 🚀"
    trial = EuTrial(
        eudract_number="123",
        url_source="http://test.com",
        sponsor_name=unicode_str,
    )
    assert trial.sponsor_name == unicode_str


def test_eu_trial_extra_fields() -> None:
    """
    Test handling of extra fields.
    By default, Pydantic v2 ignores extra fields unless Config is set to 'forbid'.
    """
    trial = EuTrial(
        eudract_number="123",
        url_source="http://test.com",
        unexpected_field="should_be_ignored",  # type: ignore[call-arg]
    )
    # Ensure the object is created successfully
    assert trial.eudract_number == "123"
    # Ensure the extra field is not part of the model's dict representation (by default)
    # Note: Pydantic v2 behavior depends on strict config, but usually ignores.
    assert not hasattr(trial, "unexpected_field")


def test_eu_trial_empty_strings_vs_none() -> None:
    """Test the difference between passing empty string and None for Optional[str]."""
    # Case 1: Passing None
    trial_none = EuTrial(
        eudract_number="1", url_source="http://t.com", trial_title=None
    )
    assert trial_none.trial_title is None

    # Case 2: Passing empty string
    trial_empty = EuTrial(
        eudract_number="2", url_source="http://t.com", trial_title=""
    )
    assert trial_empty.trial_title == ""
