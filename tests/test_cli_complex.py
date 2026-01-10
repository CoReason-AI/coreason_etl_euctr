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

import pytest

from coreason_etl_euctr.main import main


def test_cli_invalid_mode() -> None:
    """Test that providing an invalid mode to 'load' causes an error."""
    # argparse usually calls sys.exit(2) on error.
    with patch("sys.argv", ["euctr-etl", "load", "--mode", "INVALID_MODE"]):
        with pytest.raises(SystemExit) as exc:
            main()
        # Exit code 2 is standard for argparse errors
        assert exc.value.code == 2


def test_cli_invalid_page_type() -> None:
    """Test that providing a non-integer page number causes an error."""
    with patch("sys.argv", ["euctr-etl", "crawl", "--start-page", "not_an_int"]):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2


def test_cli_nonexistent_input_dir() -> None:
    """
    Test that 'load' handles a nonexistent input directory gracefully.
    run_silver logs an error and returns, main returns 0 (success execution, just no work).
    """
    with patch("sys.argv", ["euctr-etl", "load", "--input-dir", "/non/existent/path"]):
        with patch("coreason_etl_euctr.main.run_silver") as mock_run:
            # We want to actually CALL run_silver's logic?
            # No, run_silver logic is tested in test_main.py.
            # Here we test that CLI passes arguments correctly.
            # But if we want to test "handling", we rely on unit tests.
            # Let's just verify invocation here.
            ret = main()
            assert ret == 0
            mock_run.assert_called_with(input_dir="/non/existent/path", mode="FULL", storage_backend=None)


def test_cli_unknown_command() -> None:
    """Test that an unknown command prints help and returns 1? Or argparse handles it."""
    # argparse handles unknown commands as invalid choices if we used subparsers strictly?
    # No, we used dest='command'.
    with patch("sys.argv", ["euctr-etl", "unknown_cmd"]):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2
