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
from typing import IO, Any, Optional

import psycopg
from coreason_etl_euctr.loader import BaseLoader
from loguru import logger


class PostgresLoader(BaseLoader):
    """
    PostgreSQL implementation of the BaseLoader.
    Uses native `COPY` command for bulk loading.
    """

    def __init__(self) -> None:
        self.conn: Optional[psycopg.Connection[Any]] = None

    def connect(self) -> None:
        """
        Connect to PostgreSQL using environment variables:
        PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.
        """
        try:
            # psycopg automatically reads standard PG env vars if no args provided,
            # but we can also pass them explicitly if needed.
            # We rely on libpq/psycopg env var handling or explicit args if we wanted.
            # Here we assume standard env vars or user provides them.
            # Explicitly retrieving them allows for validation/logging if needed,
            # but usually passing nothing to connect() is sufficient if env vars are set.
            # However, for robustness, let's allow psycopg to handle it but ensure we catch errors.
            self.conn = psycopg.connect()
            logger.info("Successfully connected to PostgreSQL.")
        except psycopg.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self) -> None:
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Connection closed.")

    def prepare_schema(self) -> None:
        """
        Create tables: eu_trials, eu_trial_drugs, eu_trial_conditions.
        """
        if not self.conn:
            raise RuntimeError("Database not connected. Call connect() first.")

        # SQL definitions from FRD Phase 4
        # We use IF NOT EXISTS to be idempotent.
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS eu_trials (
                eudract_number VARCHAR(20) PRIMARY KEY,
                sponsor_name VARCHAR(500),
                trial_title TEXT,
                start_date DATE,
                trial_status VARCHAR(50),
                url_source TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS eu_trial_drugs (
                id SERIAL PRIMARY KEY,
                eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number),
                drug_name VARCHAR(255),
                active_ingredient VARCHAR(255),
                cas_number VARCHAR(50),
                pharmaceutical_form VARCHAR(255)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS eu_trial_conditions (
                id SERIAL PRIMARY KEY,
                eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number),
                condition_name TEXT,
                meddra_code VARCHAR(50)
            );
            """,
        ]

        try:
            with self.conn.cursor() as cur:
                for sql in ddl_statements:
                    cur.execute(sql)
            self.commit()
            logger.info("Schema preparation complete.")
        except psycopg.Error as e:
            self.rollback()
            logger.error(f"Schema preparation failed: {e}")
            raise

    def bulk_load_stream(self, data_stream: IO[str], target_table: str) -> None:
        """
        Execute COPY FROM STDIN.
        Assumes CSV format with Header.
        """
        if not self.conn:
            raise RuntimeError("Database not connected.")

        sql = f"COPY {target_table} FROM STDIN WITH (FORMAT CSV, HEADER)"
        try:
            with self.conn.cursor() as cur:
                with cur.copy(sql) as copy:
                    while data := data_stream.read(8192):
                        copy.write(data)
            logger.info(f"Bulk loaded data into {target_table}.")
        except psycopg.Error as e:
            logger.error(f"Bulk load failed for {target_table}: {e}")
            raise

    def commit(self) -> None:
        """Commit transaction."""
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        """Rollback transaction."""
        if self.conn:
            self.conn.rollback()
