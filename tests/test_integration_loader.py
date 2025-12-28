# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from datetime import date
from typing import Generator

import pytest
from coreason_etl_euctr.models import EuTrial, EuTrialCondition, EuTrialDrug
from coreason_etl_euctr.pipeline import Pipeline
from coreason_etl_euctr.postgres_loader import PostgresLoader
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")  # type: ignore[misc]
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Spins up a PostgreSQL container for integration testing.
    """
    with PostgresContainer("postgres:15", driver=None) as postgres:
        yield postgres


@pytest.fixture  # type: ignore[misc]
def db_loader(postgres_container: PostgresContainer, monkeypatch: pytest.MonkeyPatch) -> PostgresLoader:
    """
    Configures the environment variables to point to the test container
    and returns a connected PostgresLoader.
    """
    # Set environment variables for psycopg
    monkeypatch.setenv("PGHOST", postgres_container.get_container_host_ip())
    monkeypatch.setenv("PGPORT", postgres_container.get_exposed_port(5432))
    monkeypatch.setenv("PGUSER", postgres_container.username)
    monkeypatch.setenv("PGPASSWORD", postgres_container.password)
    monkeypatch.setenv("PGDATABASE", postgres_container.dbname)

    loader = PostgresLoader()
    return loader


def test_postgres_loader_full_lifecycle(db_loader: PostgresLoader) -> None:
    """
    Integration test validating the full lifecycle:
    1. Schema Creation
    2. FULL Load (Bulk Copy)
    3. Verification
    4. UPSERT Load (Incremental)
    5. Final Verification
    """
    pipeline = Pipeline()

    # 1. Connect and Prepare Schema
    db_loader.connect()
    try:
        db_loader.prepare_schema()

        # Check tables exist
        with db_loader.conn.cursor() as cur:  # type: ignore
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = {row[0] for row in cur.fetchall()}
            assert "eu_trials" in tables
            assert "eu_trial_drugs" in tables
            assert "eu_trial_conditions" in tables

        # --- Phase 1: FULL LOAD ---
        # Prepare Data
        trial_1 = EuTrial(
            eudract_number="2024-001",
            sponsor_name="Pharma Corp",
            trial_title="Phase 1 Study",
            start_date=date(2024, 1, 1),
            trial_status="Ongoing",
            url_source="http://example.com/1",
        )
        drug_1 = EuTrialDrug(
            eudract_number="2024-001",
            drug_name="WonderDrug",
            active_ingredient="Wondrium",
            pharmaceutical_form="Tablet",
        )
        cond_1 = EuTrialCondition(
            eudract_number="2024-001",
            condition_name="Headache",
            meddra_code="12345",
        )

        # Stage and Load
        # We use the internal helper logic from main.py's _load_table, but reimplemented here or call loader directly
        # Loader expects IO[str]

        # Load Trials
        gen_trials = pipeline.stage_data([trial_1])
        # We need to adapt generator to stream.
        # Since we don't have the StringIteratorIO available (it's in main.py, not exported?),
        # we can just use io.StringIO for small data in tests.
        import io

        stream_trials = io.StringIO("".join(gen_trials))
        db_loader.bulk_load_stream(stream_trials, "eu_trials")

        # Load Drugs
        gen_drugs = pipeline.stage_data([drug_1])
        stream_drugs = io.StringIO("".join(gen_drugs))
        db_loader.bulk_load_stream(stream_drugs, "eu_trial_drugs")

        # Load Conditions
        gen_conds = pipeline.stage_data([cond_1])
        stream_conds = io.StringIO("".join(gen_conds))
        db_loader.bulk_load_stream(stream_conds, "eu_trial_conditions")

        db_loader.commit()

        # Verify Phase 1
        with db_loader.conn.cursor() as cur:  # type: ignore
            cur.execute("SELECT trial_title, sponsor_name FROM eu_trials WHERE eudract_number = '2024-001'")
            row = cur.fetchone()
            assert row == ("Phase 1 Study", "Pharma Corp")

            cur.execute("SELECT count(*) FROM eu_trial_drugs")
            assert cur.fetchone()[0] == 1

        # --- Phase 2: UPSERT ---
        # Update Trial Title and Add a new Drug
        trial_1_updated = trial_1.model_copy(update={"trial_title": "Phase 1 Study (Revised)"})
        drug_2 = EuTrialDrug(
            eudract_number="2024-001",
            drug_name="Placebo",
            active_ingredient="Sugar",
            pharmaceutical_form="Tablet",
        )

        # Upsert Trial
        gen_trials_up = pipeline.stage_data([trial_1_updated])
        stream_trials_up = io.StringIO("".join(gen_trials_up))
        db_loader.upsert_stream(stream_trials_up, "eu_trials", conflict_keys=["eudract_number"])

        # Upsert Drugs (Existing + New)
        # Note: Upsert on drugs with same key (id/name/form) should update/do nothing.
        # New key should insert.
        # Let's pass both drug_1 (unchanged) and drug_2 (new)
        gen_drugs_up = pipeline.stage_data([drug_1, drug_2])
        stream_drugs_up = io.StringIO("".join(gen_drugs_up))
        db_loader.upsert_stream(
            stream_drugs_up, "eu_trial_drugs", conflict_keys=["eudract_number", "drug_name", "pharmaceutical_form"]
        )

        db_loader.commit()

        # Verify Phase 2
        with db_loader.conn.cursor() as cur:  # type: ignore
            # Check updated title
            cur.execute("SELECT trial_title FROM eu_trials WHERE eudract_number = '2024-001'")
            assert cur.fetchone()[0] == "Phase 1 Study (Revised)"

            # Check drugs count (should be 2 now)
            cur.execute("SELECT count(*) FROM eu_trial_drugs")
            assert cur.fetchone()[0] == 2

            # Check verify specific drugs
            cur.execute("SELECT drug_name FROM eu_trial_drugs WHERE eudract_number = '2024-001' ORDER BY drug_name")
            drugs = [r[0] for r in cur.fetchall()]
            assert drugs == ["Placebo", "WonderDrug"]

    finally:
        db_loader.close()
