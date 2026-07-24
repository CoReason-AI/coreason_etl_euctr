# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import os
from unittest.mock import patch

from coreason_etl_euctr.utils.config import EpistemicConfigurationManifest


def test_default_config() -> None:
    config = EpistemicConfigurationManifest()
    assert config.app_env == "development"
    assert config.log_level == "INFO"
    assert config.target_geographies == ("GB", "DE", "BE", "3rd")
    assert config.rate_limit == 1.0
    assert config.state_file_path == "pipeline_state.json"


@patch.dict(
    os.environ,
    {
        "APP_ENV": "production",
        "LOG_LEVEL": "DEBUG",
        "TARGET_GEOGRAPHIES": '["GB", "DE"]',
        "RATE_LIMIT": "2.5",
        "STATE_FILE_PATH": "/opt/state.json",
    },
    clear=True,
)
def test_environment_overrides() -> None:
    config = EpistemicConfigurationManifest()
    assert config.app_env == "production"
    assert config.log_level == "DEBUG"
    assert config.target_geographies == ("GB", "DE")
    assert config.rate_limit == 2.5
    assert config.state_file_path == "/opt/state.json"
