# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import os
from io import StringIO
from typing import Generator

import psycopg
import pytest

from coreason_etl_euctr.postgres_loader import PostgresLoader

# These credentials must match the environment provided in instructions
DB_USER = "jules"
DB_PASS = "password"
DB_NAME = "jules_db"
DB_HOST = "localhost"
DB_PORT = "5432"
DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@pytest.fixture
def db_conn() -> Generator[psycopg.Connection, None, None]:  # type: ignore[misc]
    """Yields a raw connection for verification."""
    try:
        conn = psycopg.connect(DSN, autocommit=True)
        yield conn
        conn.close()
    except psycopg.OperationalError:
        pytest.skip("Database not available")


@pytest.fixture
def loader(db_conn: psycopg.Connection) -> Generator[PostgresLoader, None, None]:  # type: ignore[misc]
    """Yields a configured loader."""
    _loader = PostgresLoader(dsn=DSN)
    _loader.connect()
    _loader.prepare_schema()
    # Ensure clean slate
    _loader.truncate_tables()
    yield _loader
    _loader.close()


def test_prepare_schema_idempotency(loader: PostgresLoader) -> None:
    """Test that prepare_schema can run multiple times without error."""
    loader.prepare_schema()
    loader.prepare_schema()


def test_full_load_lifecycle(loader: PostgresLoader, db_conn: psycopg.Connection) -> None:
    """Test a full load (Bulk Load) of trials, drugs, and conditions."""

    # 1. Load Parent
    trials_data = "eudract_number,sponsor_name\n2021-001,SponsorA\n2021-002,SponsorB"
    loader.bulk_load_stream("eu_trials", StringIO(trials_data), ["eudract_number", "sponsor_name"])

    # 2. Load Children
    drugs_data = "eudract_number,drug_name\n2021-001,DrugA"
    loader.bulk_load_stream("eu_trial_drugs", StringIO(drugs_data), ["eudract_number", "drug_name"])

    # Verify
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM eu_trials")
        assert cur.fetchone()[0] == 2

        cur.execute("SELECT count(*) FROM eu_trial_drugs")
        assert cur.fetchone()[0] == 1


def test_truncate_tables(loader: PostgresLoader, db_conn: psycopg.Connection) -> None:
    """Test that truncate clears all tables."""
    # Seed data
    trials_data = "eudract_number\n2021-001"
    loader.bulk_load_stream("eu_trials", StringIO(trials_data), ["eudract_number"])

    loader.truncate_tables()

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM eu_trials")
        assert cur.fetchone()[0] == 0


def test_upsert_stream_with_child_cleanup(loader: PostgresLoader, db_conn: psycopg.Connection) -> None:
    """Test that upserting a parent trial clears its old children."""

    # 1. Initial State: Trial 001 with DrugA
    loader.bulk_load_stream(
        "eu_trials",
        StringIO("eudract_number,sponsor_name\n2021-001,OldSponsor"),
        ["eudract_number", "sponsor_name"]
    )
    loader.bulk_load_stream(
        "eu_trial_drugs",
        StringIO("eudract_number,drug_name\n2021-001,DrugA"),
        ["eudract_number", "drug_name"]
    )

    # 2. Verify Initial State
    with db_conn.cursor() as cur:
        cur.execute("SELECT sponsor_name FROM eu_trials WHERE eudract_number = '2021-001'")
        assert cur.fetchone()[0] == "OldSponsor"
        cur.execute("SELECT count(*) FROM eu_trial_drugs WHERE eudract_number = '2021-001'")
        assert cur.fetchone()[0] == 1

    # 3. Upsert: Update Sponsor to NewSponsor
    # PostgresLoader.upsert_stream should handle child cleanup for 'eu_trials'
    upsert_data = StringIO("eudract_number,sponsor_name\n2021-001,NewSponsor")
    loader.upsert_stream(
        "eu_trials",
        upsert_data,
        ["eudract_number", "sponsor_name"],
        conflict_keys=["eudract_number"]
    )

    # 4. Verify Parent Updated and Children Cleared
    with db_conn.cursor() as cur:
        # Parent updated
        cur.execute("SELECT sponsor_name FROM eu_trials WHERE eudract_number = '2021-001'")
        assert cur.fetchone()[0] == "NewSponsor"

        # Children cleared (because upsert_stream deletes them)
        cur.execute("SELECT count(*) FROM eu_trial_drugs WHERE eudract_number = '2021-001'")
        assert cur.fetchone()[0] == 0

    # 5. Load New Children (simulating Pipeline flow)
    loader.bulk_load_stream(
        "eu_trial_drugs",
        StringIO("eudract_number,drug_name\n2021-001,DrugB"),
        ["eudract_number", "drug_name"]
    )

    with db_conn.cursor() as cur:
        cur.execute("SELECT drug_name FROM eu_trial_drugs WHERE eudract_number = '2021-001'")
        assert cur.fetchone()[0] == "DrugB"
