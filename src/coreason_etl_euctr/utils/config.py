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
AGENT INSTRUCTION: This module provides a 12-Factor App compliant configuration
management system utilizing environment variables for all environment-specific configurations.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EpistemicConfigurationManifest(BaseSettings):
    """
    Mathematical boundary contract representing the system configuration.
    It isolates environment variables from business logic.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Logging and Observability
    app_env: str = Field("development", description="The application environment (development, testing, production).")
    log_level: str = Field("INFO", description="The logging level (e.g., DEBUG, INFO, WARNING, ERROR).")

    # Pipeline Operation Settings
    target_geographies: tuple[str, ...] = Field(
        default=("GB", "DE", "BE", "3rd"),
        description="The tuple of target country codes to process.",
    )
    rate_limit: float = Field(1.0, description="Politeness delay in seconds between HTTP requests.")

    # State Management
    state_file_path: str = Field(
        "pipeline_state.json",
        description="Path to the local state management JSON file.",
    )


# Singleton instantiation for global access
settings = EpistemicConfigurationManifest()
