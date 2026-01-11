# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from unittest.mock import MagicMock, patch

from coreason_etl_euctr.main import run_silver
from coreason_etl_euctr.storage import StorageObject
from coreason_etl_euctr.worker import process_file_content


def test_process_file_content_valid() -> None:
    """Test that the worker function correctly parses valid content."""
    # Simplified HTML content
    content = """
    <table>
        <tr><td>EudraCT Number:</td><td>2020-123456-78</td></tr>
        <tr><td>Name of Sponsor:</td><td>Acme Corp</td></tr>
        <tr><td>Full title of the trial:</td><td>Test Trial</td></tr>
        <tr><td>Date of Competent Authority Decision:</td><td>2020-01-01</td></tr>
        <tr><td>Trade name:</td><td>WonderDrug</td></tr>
        <tr><td>Medical condition:</td><td>Headache</td></tr>
    </table>
    """
    key = "2020-123456-78.html"
    config = {"type": "mock", "content": content}

    mock_storage = MagicMock()
    mock_storage.read.return_value = content

    with patch("coreason_etl_euctr.worker.create_storage_backend", return_value=mock_storage):
        result = process_file_content(key, config)

    assert result is not None
    trial, drugs, conds = result

    assert trial.eudract_number == "2020-123456-78"
    assert trial.sponsor_name == "Acme Corp"
    assert len(drugs) == 1
    assert drugs[0].drug_name == "WonderDrug"
    assert len(conds) == 1
    assert conds[0].condition_name == "Headache"


def test_process_file_content_invalid_id() -> None:
    """Test that the worker function handles ID mismatch."""
    content = """
    <table>
        <tr><td>EudraCT Number:</td><td>2020-999999-99</td></tr>
    </table>
    """
    key = "2020-123456-78.html"  # Mismatch
    config = {"type": "mock", "content": content}

    mock_storage = MagicMock()
    mock_storage.read.return_value = content

    # We expect a warning log. Patch logger to verify.
    with (
        patch("coreason_etl_euctr.worker.logger") as mock_logger,
        patch("coreason_etl_euctr.worker.create_storage_backend", return_value=mock_storage),
    ):
        # Bind returns a new logger proxy, so we need to mock the result of bind too
        # logger.bind(...) returns context_logger
        # context_logger.warning(...)

        mock_context_logger = MagicMock()
        mock_logger.bind.return_value = mock_context_logger

        result = process_file_content(key, config)

        assert result is not None
        trial, _, _ = result
        assert trial.eudract_number == "2020-999999-99"

        # Verify warning call
        mock_context_logger.warning.assert_called()
        args = mock_context_logger.warning.call_args[0]
        assert "mismatch" in args[0]


def test_process_file_content_generic_error() -> None:
    """Test generic exception handling."""
    content = "<html>Content</html>"
    key = "2020-123456-78.html"
    config = {"type": "mock", "content": content}

    mock_storage = MagicMock()
    mock_storage.read.return_value = content

    # Patch Parser to raise generic Exception
    with (
        patch("coreason_etl_euctr.worker.Parser") as MockParser,
        patch("coreason_etl_euctr.worker.create_storage_backend", return_value=mock_storage),
    ):
        mock_instance = MockParser.return_value
        mock_instance.parse_trial.side_effect = Exception("Boom")

        result = process_file_content(key, config)
        assert result is None


def test_process_file_content_parse_error() -> None:
    """Test handling of parse errors (missing ID)."""
    content = "<html>Empty</html>"
    key = "2020-123456-78.html"
    config = {"type": "mock", "content": content}

    mock_storage = MagicMock()
    mock_storage.read.return_value = content

    with patch("coreason_etl_euctr.worker.create_storage_backend", return_value=mock_storage):
        result = process_file_content(key, config)
    assert result is None


def test_run_silver_parallel_execution() -> None:
    """
    Test run_silver with mocked executor to verify flow.
    We can't easily assert parallelism, but we check if it aggregates results correctly.
    """
    mock_storage = MagicMock()
    mock_storage.list_files.return_value = [
        StorageObject(key="2020-001.html", mtime=1000),
        StorageObject(key="2020-002.html", mtime=1000),
    ]
    # In run_silver, we now only need get_config
    mock_storage.get_config.return_value = {"type": "mock"}

    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None

    mock_loader = MagicMock()

    # We mock ProcessPoolExecutor to run synchronously or return futures
    # But simpler: Mock process_file_content to return dummy data
    # and let the real executor run (it works with mocks if we patch where it is imported).

    # Since process_file_content is imported in main, we patch it there.
    # Also ProcessPoolExecutor in main.

    with patch("coreason_etl_euctr.main.concurrent.futures.ProcessPoolExecutor") as MockExecutor:
        # Setup mock executor
        executor_instance = MockExecutor.return_value
        executor_instance.__enter__.return_value = executor_instance

        # We need to mock submit to return a future
        mock_future1 = MagicMock()
        mock_future1.result.return_value = (MagicMock(eudract_number="2020-001"), [MagicMock()], [MagicMock()])

        mock_future2 = MagicMock()
        mock_future2.result.return_value = (MagicMock(eudract_number="2020-002"), [], [])

        executor_instance.submit.side_effect = [mock_future1, mock_future2]

        # We need to mock as_completed
        with patch("coreason_etl_euctr.main.concurrent.futures.as_completed") as mock_as_completed:
            mock_as_completed.return_value = [mock_future1, mock_future2]

            run_silver(pipeline=mock_pipeline, loader=mock_loader, storage_backend=mock_storage, mode="FULL")

            # Verify submit was called twice
            assert executor_instance.submit.call_count == 2

            # Verify loader called
            assert mock_loader.truncate_tables.called
            assert mock_loader.bulk_load_stream.call_count == 3  # Trials, Drugs, Conditions
