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
from typing import List, Optional

from pydantic import BaseModel, Field


class EuTrialDrug(BaseModel):
    """
    Represents a single drug used in a clinical trial (One-to-Many).
    Corresponds to Section D of the EU CTR protocol.
    """

    drug_name: Optional[str] = Field(None, description="Trade name of the drug")
    active_ingredient: Optional[str] = Field(None, description="Name of the active substance")
    pharmaceutical_form: Optional[str] = Field(None, description="Pharmaceutical form (e.g. Tablet)")
    cas_number: Optional[str] = Field(None, description="CAS Number")


class EuTrialCondition(BaseModel):
    """
    Represents a medical condition investigated in the trial (One-to-Many).
    Corresponds to Section E of the EU CTR protocol.
    """

    condition_name: Optional[str] = Field(None, description="Medical condition being investigated")
    meddra_code: Optional[str] = Field(None, description="MedDRA version/code")


class EuTrial(BaseModel):
    """
    Represents the core clinical trial entity.
    Corresponds to the main header and Sections A, B of the EU CTR protocol.
    """

    eudract_number: str = Field(..., description="Primary Key: The unique EudraCT number")
    sponsor_name: Optional[str] = Field(None, description="Name of the sponsor")
    trial_title: Optional[str] = Field(None, description="Full title of the trial")
    start_date: Optional[date] = Field(
        None, description="Date of Competent Authority Decision or Date record first entered"
    )
    trial_status: Optional[str] = Field(None, description="Status of the trial (e.g. Completed)")
    url_source: Optional[str] = Field(None, description="The URL from which this record was scraped")
    last_updated: datetime = Field(
        default_factory=datetime.now, description="Timestamp of when this record was processed"
    )

    # Relationship fields (will be flattened during loading)
    drugs: List[EuTrialDrug] = Field(default_factory=list, description="List of drugs involved in the trial")
    conditions: List[EuTrialCondition] = Field(
        default_factory=list, description="List of medical conditions being investigated"
    )
