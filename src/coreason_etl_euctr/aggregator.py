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
AGENT INSTRUCTION: This module defines the EpistemicGoldAggregatorTask to perform
high-performance transformations via Polars, implementing text cleaning and field projection.
"""

import uuid
from typing import Any

import polars as pl

NAMESPACE_EUCTR = uuid.uuid5(uuid.NAMESPACE_URL, "https://www.clinicaltrialsregister.eu")


class EpistemicGoldAggregatorTask:
    """
    Manages the transformation of parsed Silver JSON into Gold Polars DataFrame.
    """

    @staticmethod
    def _generate_uuid5(expr: pl.Expr) -> pl.Expr:
        """
        Generates UUIDv5 sequence deterministically based on NAMESPACE_EUCTR.

        Args:
            expr: A Polars Expr containing strings (e.g., EudraCT Numbers).

        Returns:
            A Polars Expr containing the generated UUIDv5 strings.
        """

        def to_uuid5(val: str | None) -> str | None:
            if not val:
                return None
            return str(uuid.uuid5(NAMESPACE_EUCTR, str(val)))

        return expr.map_batches(lambda s: s.map_elements(to_uuid5, return_dtype=pl.Utf8))

    def clean_text(self, df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        """
        Cleans text columns by stripping HTML tags, removing non-breaking spaces,
        and normalizing whitespace.

        Args:
            df: Polars DataFrame to clean.
            cols: List of column names to apply text cleaning.

        Returns:
            Cleaned Polars DataFrame.
        """
        for col in cols:
            if col in df.columns and df.schema[col] == pl.Utf8:
                # 1. Replace &nbsp; and \xa0 with space
                # 2. Strip HTML tags <...>
                # 3. Replace multiple spaces with a single space
                # 4. Strip leading/trailing whitespaces
                df = df.with_columns(
                    pl.col(col)
                    .str.replace_all(r"&nbsp;|\xa0", " ")
                    .str.replace_all(r"<[^>]*>", "")
                    .str.replace_all(r"\s+", " ")
                    .str.strip_chars()
                )
        return df

    def aggregate(self, silver_data: list[dict[str, Any]]) -> pl.DataFrame:
        """
        Converts Silver JSON dictionaries into a Gold Polars DataFrame with required fields.

        Args:
            silver_data: A list of dictionaries parsed from HTML.

        Returns:
            Polars DataFrame with projected fields.
        """
        if not silver_data:
            return pl.DataFrame()

        # Base DataFrame
        df = pl.DataFrame(silver_data)

        # Fields to project
        core_fields = ["A.2", "A.3", "B.1.1", "E.1.1.2", "E.2.1", "E.2.2", "E.3", "E.4", "E.5.1", "E.5.2"]

        # Select only required columns if they exist in the DataFrame,
        # otherwise create them with null values
        projection = []
        for field in core_fields:
            if field in df.columns:
                if field == "A.2":
                    projection.append(pl.col(field).alias("source_id"))
                else:
                    projection.append(pl.col(field))
            else:
                if field == "A.2":
                    projection.append(pl.lit(None).alias("source_id").cast(pl.Utf8))
                else:
                    projection.append(pl.lit(None).alias(field).cast(pl.Utf8))

        # Perform projection
        df_projected = df.select(projection)

        # Generate coreason_id using UUIDv5 on source_id
        df_projected = df_projected.with_columns(pl.col("source_id").pipe(self._generate_uuid5).alias("coreason_id"))

        # Clean text columns (note: A.2 is now source_id)
        clean_fields = ["source_id"] + [f for f in core_fields if f != "A.2"]
        return self.clean_text(df_projected, clean_fields)
