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
from typing import Optional

from pydantic import BaseModel, Field


class EuTrial(BaseModel):
    """
    Silver layer representation of the core Trial table.
    """

    eudract_number: str = Field(..., description="The EudraCT number, acts as Primary Key.")
    sponsor_name: Optional[str] = Field(None, description="Name of the Sponsor.")
    trial_title: Optional[str] = Field(None, description="Full title of the trial.")
    start_date: Optional[date] = Field(
        None, description="Date of Competent Authority Decision or Date record first entered."
    )
    trial_status: Optional[str] = Field(None, description="Status of the trial (e.g., Completed, Prematurely Ended).")
    url_source: str = Field(..., description="The source URL from which this record was scraped.")
    last_updated: datetime = Field(
        default_factory=datetime.now, description="Timestamp when this record was processed."
    )


class EuTrialDrug(BaseModel):
    """
    Silver layer representation of a Drug used in a trial.
    One-to-Many relationship with EuTrial.
    """

    eudract_number: str = Field(..., description="Foreign Key to EuTrial.")
    drug_name: Optional[str] = Field(None, description="Trade name of the drug.")
    active_ingredient: Optional[str] = Field(None, description="Name of the active substance.")
    cas_number: Optional[str] = Field(None, description="CAS number if available.")
    pharmaceutical_form: Optional[str] = Field(None, description="Pharmaceutical form (e.g., Tablet).")


class EuTrialCondition(BaseModel):
    """
    Silver layer representation of a Medical Condition being investigated.
    One-to-Many relationship with EuTrial.
    """

    eudract_number: str = Field(..., description="Foreign Key to EuTrial.")
    condition_name: Optional[str] = Field(None, description="Medical condition(s) being investigated.")
    meddra_code: Optional[str] = Field(None, description="MedDRA version/level codes.")
