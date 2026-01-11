# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_euctr.main import main


@pytest.fixture  # type: ignore[misc]
def mock_loaders() -> Generator[tuple[MagicMock, MagicMock, MagicMock, MagicMock], None, None]:
    with (
        patch("coreason_etl_euctr.main.PostgresLoader") as mock_pg,
        patch("coreason_etl_euctr.main.RedshiftLoader") as mock_rs,
        patch("coreason_etl_euctr.main.BigQueryLoader") as mock_bq,
        patch("coreason_etl_euctr.main.run_silver") as mock_run,
    ):
        yield mock_pg, mock_rs, mock_bq, mock_run


def test_cli_load_bigquery(mock_loaders: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, mock_bq, mock_run = mock_loaders

    with patch(
        "sys.argv",
        [
            "main.py",
            "load",
            "--target-db",
            "bigquery",
            "--gcs-bucket",
            "my-bucket",
            "--gcs-prefix",
            "my-prefix",
            "--bq-project",
            "my-project",
            "--bq-dataset",
            "my_dataset",
        ],
    ):
        assert main() == 0

        mock_bq.assert_called_once_with(
            project_id="my-project", dataset_id="my_dataset", gcs_bucket="my-bucket", gcs_prefix="my-prefix"
        )
        mock_run.assert_called_once()


def test_cli_load_bigquery_env_vars(mock_loaders: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, mock_bq, mock_run = mock_loaders

    with (
        patch("sys.argv", ["main.py", "load", "--target-db", "bigquery"]),
        patch.dict(
            "os.environ",
            {"EUCTR_GCS_BUCKET": "env-bucket", "EUCTR_GCS_PREFIX": "env-prefix", "EUCTR_BQ_PROJECT": "env-project"},
        ),
    ):
        assert main() == 0

        mock_bq.assert_called_once_with(
            project_id="env-project",
            dataset_id="eu_ctr",  # Default
            gcs_bucket="env-bucket",
            gcs_prefix="env-prefix",
        )


def test_cli_load_bigquery_missing_bucket(mock_loaders: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
    _, _, _, _ = mock_loaders

    # No bucket arg and no env var
    with patch("sys.argv", ["main.py", "load", "--target-db", "bigquery"]):
        assert main() == 1
