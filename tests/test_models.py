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


def test_eu_trial_valid_full() -> None:
    """Test creating a valid EuTrial with all fields and children."""
    trial = EuTrial(
        eudract_number="2021-123456-78",
        sponsor_name="Acme Corp",
        trial_title="A study of X",
        start_date=date(2021, 1, 1),
        trial_status="Completed",
        url_source="https://example.com/trial",
        drugs=[
            EuTrialDrug(drug_name="Drug A", active_ingredient="Substance A"),
            EuTrialDrug(drug_name="Drug B", pharmaceutical_form="Tablet"),
        ],
        conditions=[
            EuTrialCondition(condition_name="Flu", meddra_code="1001"),
        ],
    )

    assert trial.eudract_number == "2021-123456-78"
    assert trial.start_date == date(2021, 1, 1)
    assert len(trial.drugs) == 2
    assert len(trial.conditions) == 1
    assert trial.drugs[0].drug_name == "Drug A"
    assert trial.conditions[0].condition_name == "Flu"
    assert isinstance(trial.last_updated, datetime)


def test_eu_trial_minimal() -> None:
    """Test creating a valid EuTrial with only required fields."""
    trial = EuTrial(eudract_number="2021-000000-00")
    assert trial.eudract_number == "2021-000000-00"
    assert trial.drugs == []
    assert trial.conditions == []
    assert trial.sponsor_name is None


def test_eu_trial_missing_pk() -> None:
    """Test that missing the primary key (eudract_number) raises ValidationError."""
    with pytest.raises(ValidationError):
        EuTrial(sponsor_name="No ID")  # type: ignore[call-arg]


def test_eu_trial_invalid_date() -> None:
    """Test that invalid date strings raise ValidationError."""
    with pytest.raises(ValidationError):
        EuTrial(
            eudract_number="123",
            start_date="not-a-date",  # type: ignore[arg-type]
        )


def test_nested_models() -> None:
    """Test validation of nested models."""
    drug = EuTrialDrug(drug_name="Aspirin")
    assert drug.drug_name == "Aspirin"
    assert drug.active_ingredient is None

    condition = EuTrialCondition(condition_name="Headache")
    assert condition.condition_name == "Headache"
    assert condition.meddra_code is None
