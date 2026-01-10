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
from coreason_etl_euctr.models import EuTrial
from coreason_etl_euctr.storage import StorageObject
from coreason_etl_euctr.worker import process_file_content


def test_worker_complex_html() -> None:
    """
    Test process_file_content with complex HTML containing:
    - Unicode characters
    - Multiple drugs
    - Multiple conditions
    - Nested structure (simulated via simple table for now as parser uses regex/label search)
    """
    content = """
    <html>
    <body>
        <table>
            <tr>
                <td>EudraCT Number:</td>
                <td>2021-000123-45</td>
            </tr>
            <tr>
                <td>Name of Sponsor:</td>
                <td>Sponsor Üñíçødé</td>
            </tr>
            <tr>
                <td>Full title of the trial:</td>
                <td>Study of Drug A vs Drug B</td>
            </tr>
            <tr>
                <td>Date of Competent Authority Decision:</td>
                <td>2021-06-15</td>
            </tr>
        </table>

        <!-- Drug 1 -->
        <table>
            <tr><td>Trade name:</td><td>Drug Alpha α</td></tr>
            <tr><td>Name of Active Substance:</td><td>Substance A</td></tr>
            <tr><td>Pharmaceutical form:</td><td>Tablet</td></tr>
            <tr><td>CAS Number:</td><td>123-45-6</td></tr>
        </table>

        <!-- Drug 2 -->
        <table>
            <tr><td>Trade name:</td><td>Drug Beta β</td></tr>
            <tr><td>Name of Active Substance:</td><td>Substance B</td></tr>
            <tr><td>Pharmaceutical form:</td><td>Injection</td></tr>
            <tr><td>CAS Number:</td><td>987-65-4</td></tr>
        </table>

        <!-- Conditions -->
        <table>
            <tr>
                <td>Medical condition:</td>
                <td>Chronic Pain</td>
            </tr>
            <tr>
                <td>MedDRA version:</td>
                <td>24.0</td>
            </tr>
            <tr>
                <td>MedDRA level:</td>
                <td>PT</td>
            </tr>
        </table>
    </body>
    </html>
    """

    key = "2021-000123-45.html"
    source = "file://source"

    result = process_file_content(content, key, source)
    assert result is not None

    trial, drugs, conds = result

    # Verify Trial
    assert trial.eudract_number == "2021-000123-45"
    assert trial.sponsor_name == "Sponsor Üñíçødé"
    assert str(trial.start_date) == "2021-06-15"

    # Verify Drugs (Should be 2)
    assert len(drugs) == 2
    drug_names = {d.drug_name for d in drugs}
    assert "Drug Alpha α" in drug_names
    assert "Drug Beta β" in drug_names

    # Verify Conditions
    assert len(conds) == 1
    assert conds[0].condition_name == "Chronic Pain"
    assert "24.0" in str(conds[0].meddra_code)


def test_orchestration_aggregation_diverse() -> None:
    """
    Test run_silver aggregation with a mix of results:
    1. Full success (Trial + Drugs + Conds)
    2. Partial success (Trial only, no children)
    3. Failure (None)
    """
    mock_pipeline = MagicMock()
    mock_pipeline.get_silver_watermark.return_value = None
    mock_pipeline.stage_data.return_value = iter(["header"])

    mock_loader = MagicMock()

    # Create valid trial objects
    t1 = EuTrial(eudract_number="T1", url_source="s")
    t2 = EuTrial(eudract_number="T2", url_source="s")

    # Mock futures
    # Future 1: T1 with children
    f1 = MagicMock()
    f1.result.return_value = (t1, [MagicMock(drug_name="D1")], [MagicMock(condition_name="C1")])

    # Future 2: Failure (None)
    f2 = MagicMock()
    f2.result.return_value = None

    # Future 3: T2 with no children
    f3 = MagicMock()
    f3.result.return_value = (t2, [], [])

    # Mock Storage to return 3 files
    mock_storage = MagicMock()
    mock_storage.list_files.return_value = [
        StorageObject(key="1.html", mtime=100),
        StorageObject(key="2.html", mtime=100),
        StorageObject(key="3.html", mtime=100),
    ]
    mock_storage.read.return_value = "content"

    with (
        patch("coreason_etl_euctr.main.concurrent.futures.ProcessPoolExecutor") as MockExecutor,
        patch("coreason_etl_euctr.main.concurrent.futures.as_completed") as mock_as_completed,
    ):
        executor_instance = MockExecutor.return_value
        executor_instance.__enter__.return_value = executor_instance

        # submit must return the futures that as_completed will yield, so the dictionary lookup works.
        executor_instance.submit.side_effect = [f1, f2, f3]

        # as_completed yields the futures we defined
        mock_as_completed.return_value = [f1, f2, f3]

        run_silver(input_dir="dummy", storage_backend=mock_storage, pipeline=mock_pipeline, loader=mock_loader)

    # Verification

    # Check what was passed to stage_data
    # stage_data is called 3 times (trials, drugs, conditions)
    assert mock_pipeline.stage_data.call_count == 3

    # We need to inspect the calls to see the lists passed
    calls = mock_pipeline.stage_data.call_args_list

    # Call 1: Trials
    # trials list should contain T1 and T2
    trials_arg = calls[0][0][0]  # 1st call, 1st arg
    assert len(trials_arg) == 2
    ids = {t.eudract_number for t in trials_arg}
    assert "T1" in ids
    assert "T2" in ids

    # Call 2: Drugs
    # drugs list should contain D1 only
    drugs_arg = calls[1][0][0]
    assert len(drugs_arg) == 1
    assert drugs_arg[0].drug_name == "D1"

    # Call 3: Conditions
    # conditions list should contain C1 only
    conds_arg = calls[2][0][0]
    assert len(conds_arg) == 1
    assert conds_arg[0].condition_name == "C1"
