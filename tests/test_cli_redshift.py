# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import patch

from coreason_etl_euctr.main import main


class TestCLIRedshift:
    def test_load_redshift_args(self) -> None:
        """Verify that CLI args correctly instantiate RedshiftLoader."""
        with (
            patch(
                "sys.argv",
                [
                    "main.py",
                    "load",
                    "--target-db",
                    "redshift",
                    "--s3-bucket",
                    "my-bucket",
                    "--iam-role",
                    "arn:aws:iam::123:role/MyRole",
                ],
            ),
            patch("coreason_etl_euctr.main.run_silver") as mock_run_silver,
            patch("coreason_etl_euctr.main.RedshiftLoader") as mock_loader_cls,
        ):
            ret = main()
            assert ret == 0

            # Verify Loader instantiation
            mock_loader_cls.assert_called_once_with(
                s3_bucket="my-bucket", s3_prefix="", region=None, iam_role="arn:aws:iam::123:role/MyRole"
            )

            # Verify run_silver call
            mock_run_silver.assert_called_once()
            _, kwargs = mock_run_silver.call_args
            assert kwargs["loader"] == mock_loader_cls.return_value

    def test_load_redshift_missing_bucket(self) -> None:
        """Verify error when S3 bucket missing for Redshift."""
        with (
            patch("sys.argv", ["main.py", "load", "--target-db", "redshift"]),
            patch("coreason_etl_euctr.main.run_silver") as mock_run_silver,
        ):
            # Should fail because no bucket provided
            ret = main()
            assert ret == 1
            mock_run_silver.assert_not_called()

    def test_load_postgres_default(self) -> None:
        """Verify default is Postgres."""
        with (
            patch("sys.argv", ["main.py", "load"]),
            patch("coreason_etl_euctr.main.run_silver"),
            patch("coreason_etl_euctr.main.PostgresLoader") as mock_pg_loader,
        ):
            ret = main()
            assert ret == 0
            mock_pg_loader.assert_called_once()
