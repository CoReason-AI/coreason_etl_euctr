# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import pytest
from coreason_etl_euctr.models import EuTrial, EuTrialCondition, EuTrialDrug
from pydantic import ValidationError


def test_eutrial_constraints() -> None:
    """Test string length constraints for EuTrial."""
    # Valid
    EuTrial(
        eudract_number="2023-001234-56",
        sponsor_name="Short Sponsor",
        trial_status="Completed",
        url_source="http://test",
    )

    # Invalid EudraCT Number (>20 chars)
    with pytest.raises(ValidationError) as exc:
        EuTrial(
            eudract_number="A" * 21,
            url_source="http://test",
        )
    assert "String should have at most 20 characters" in str(exc.value)

    # Invalid Sponsor Name (>500 chars)
    with pytest.raises(ValidationError) as exc:
        EuTrial(
            eudract_number="2023-001",
            sponsor_name="A" * 501,
            url_source="http://test",
        )
    assert "String should have at most 500 characters" in str(exc.value)

    # Invalid Trial Status (>50 chars)
    with pytest.raises(ValidationError) as exc:
        EuTrial(
            eudract_number="2023-001",
            trial_status="S" * 51,
            url_source="http://test",
        )
    assert "String should have at most 50 characters" in str(exc.value)


def test_eutrialdrug_constraints() -> None:
    """Test string length constraints for EuTrialDrug."""
    base_data = {"eudract_number": "2023-001"}

    # Valid
    EuTrialDrug(**base_data, drug_name="Valid Name")

    # Invalid drug_name (>255)
    with pytest.raises(ValidationError):
        EuTrialDrug(**base_data, drug_name="D" * 256)

    # Invalid active_ingredient (>255)
    with pytest.raises(ValidationError):
        EuTrialDrug(**base_data, active_ingredient="I" * 256)

    # Invalid cas_number (>50)
    with pytest.raises(ValidationError):
        EuTrialDrug(**base_data, cas_number="C" * 51)

    # Invalid pharmaceutical_form (>255)
    with pytest.raises(ValidationError):
        EuTrialDrug(**base_data, pharmaceutical_form="P" * 256)


def test_eutrialcondition_constraints() -> None:
    """Test string length constraints for EuTrialCondition."""
    base_data = {"eudract_number": "2023-001"}

    # Valid
    EuTrialCondition(**base_data, condition_name="Valid Condition", meddra_code="1.0")

    # Invalid meddra_code (>50)
    with pytest.raises(ValidationError):
        EuTrialCondition(**base_data, meddra_code="M" * 51)
