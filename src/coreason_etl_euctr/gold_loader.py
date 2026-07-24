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
AGENT INSTRUCTION: This module defines the EpistemicGoldLoaderTask to manage
the ingestion of flattened Polars DataFrames into the Gold layer via dlt.
It also provides the EpistemicGoldManifest for strict Pydantic validation.
"""

from typing import Any

import dlt
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from coreason_etl_euctr.utils.logger import logger


class EpistemicGoldManifest(BaseModel):
    """
    Mathematical boundary contract representing the validated Gold row for the EU CTR Knowledge Graph.
    Ensures that required identities and structures are present before persistence.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    coreason_id: str = Field(description="Generated UUIDv5 identifier based on the EudraCT Number.")
    source_id: str = Field(description="The primary EudraCT Number extracted from section A.2.")

    A_3: str | None = Field(None, description="A.3 - Full Title of the Trial.", alias="A.3")
    B_1_1: str | None = Field(None, description="B.1.1 - Name of Sponsor.", alias="B.1.1")
    trial_status_coalesced: str | None = Field(None, description="Coalesced localization statuses of the trial.")

    E_1_1_2: str | None = Field(None, description="E.1.1.2 - Therapeutic Area.", alias="E.1.1.2")
    E_2_1: str | None = Field(None, description="E.2.1 - Main Objective of the trial.", alias="E.2.1")
    E_2_2: str | None = Field(None, description="E.2.2 - Secondary Objectives of the trial.", alias="E.2.2")
    E_3: str | None = Field(None, description="E.3 - Principal Inclusion Criteria.", alias="E.3")
    E_4: str | None = Field(None, description="E.4 - Principal Exclusion Criteria.", alias="E.4")
    E_5_1: str | None = Field(None, description="E.5.1 - Primary Endpoints.", alias="E.5.1")
    E_5_2: str | None = Field(None, description="E.5.2 - Secondary Endpoints.", alias="E.5.2")

    E_7_1: bool | None = Field(None, description="E.7.1 - Trial Phase I flag.", alias="E.7.1")
    E_7_2: bool | None = Field(None, description="E.7.2 - Trial Phase II flag.", alias="E.7.2")
    E_7_3: bool | None = Field(None, description="E.7.3 - Trial Phase III flag.", alias="E.7.3")
    E_7_4: bool | None = Field(None, description="E.7.4 - Trial Phase IV flag.", alias="E.7.4")

    products: str | None = Field(
        None, description="JSON array of Investigational Medicinal Products extracted from D.IMP blocks."
    )


class EpistemicGoldLoaderTask:
    """
    Manages the ingestion of validated Gold Polars DataFrames into the final destination via dlt.
    """

    def __init__(
        self, pipeline_name: str = "coreason_etl_euctr", destination: str = "duckdb", dataset_name: str = "gold"
    ) -> None:
        """
        Initializes the Gold Loader task with dlt pipeline configuration.
        """
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset_name = dataset_name

    def validate_dataframe(self, df: pl.DataFrame) -> None:
        """
        Validates the Polars DataFrame against the EpistemicGoldManifest boundary contract passively.
        It logs warnings for any validation failures but does not modify the DataFrame.

        Args:
            df: The Polars DataFrame to validate.
        """
        if df.is_empty():
            return

        # Convert Polars DataFrame to a list of dicts for Pydantic validation
        records = df.to_dicts()

        for record in records:
            try:
                # Pydantic will validate the dictionary against the defined schema aliases
                EpistemicGoldManifest.model_validate(record)
            except Exception as e:
                # Log the validation error as a warning.
                # Do not drop rows or halt the pipeline.
                logger.warning(f"Data validation failed for record {record.get('source_id', 'UNKNOWN')}: {e}")

    def load_gold_dataframe(
        self, df: pl.DataFrame, write_disposition: Any = "merge"
    ) -> dlt.common.pipeline.LoadInfo | None:
        """
        Validates and loads the Gold Polars DataFrame into the final analytical repository via dlt.

        Args:
            df: The flattened Polars DataFrame containing the projected Gold data.
            write_disposition: The dlt load mode ("replace", "append", or "merge").

        Returns:
            The dlt LoadInfo object containing the results of the load operation, or None if empty.
        """
        if df.is_empty():
            logger.info("Empty DataFrame received; skipping Gold load.")
            return None

        logger.info(f"Validating {len(df)} rows against EpistemicGoldManifest.")
        self.validate_dataframe(df)

        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset_name,
        )

        logger.info(
            f"Loading {len(df)} rows into Gold layer (coreason_etl_euctr_gold_euctr_rag) "
            f"with mode: {write_disposition}."
        )

        # We define coreason_id as the primary key for merges
        run_kwargs: dict[str, Any] = {
            "table_name": "coreason_etl_euctr_gold_euctr_rag",
            "write_disposition": write_disposition,
        }
        if write_disposition == "merge":
            run_kwargs["primary_key"] = "coreason_id"

        # dlt handles pandas/pyarrow via list of dicts or objects directly sometimes,
        # but to be completely safe against serialization issues while preserving the polars
        # specification logic:
        # Convert Polars DataFrame to a list of dicts for `dlt` to avoid serialization issues
        # that occur with nested PyArrow fields within tests and specific destination handlers.
        # This keeps Polars strictly for memory-efficient projection and logic, handing off cleanly.
        try:
            load_info = pipeline.run(df.to_dicts(), **run_kwargs)
        except TypeError:
            raise

        logger.info("Successfully loaded Gold layer.")
        return load_info
