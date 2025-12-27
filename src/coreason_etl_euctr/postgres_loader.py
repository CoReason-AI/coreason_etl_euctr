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
from typing import IO, List, Optional

import psycopg
from loguru import logger

from coreason_etl_euctr.loader import BaseLoader


class PostgresLoader(BaseLoader):
    """
    PostgreSQL implementation of the Loader using psycopg 3.
    """

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv("DATABASE_URL")
        # Fallback to individual env vars if no DSN
        if not self.dsn:  # pragma: no cover
            # Covered by unit test `test_init_env_vars` but usually with mocked env
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASS", "postgres")
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5432")
            dbname = os.getenv("DB_NAME", "postgres")
            self.dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        self.conn: Optional[psycopg.Connection] = None

    def connect(self) -> None:
        if self.conn is None or self.conn.closed:
            logger.info("Connecting to PostgreSQL...")
            self.conn = psycopg.connect(self.dsn)  # pragma: no cover
            self.conn.autocommit = False

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Closed PostgreSQL connection.")

    def prepare_schema(self) -> None:
        """
        Creates the Silver layer tables.
        """
        if not self.conn:
            self.connect()

        assert self.conn is not None

        ddl = """
        -- 1. The Core Trial Table
        CREATE TABLE IF NOT EXISTS eu_trials (
            eudract_number VARCHAR(20) PRIMARY KEY,
            sponsor_name VARCHAR(500),
            trial_title TEXT,
            start_date DATE,
            trial_status VARCHAR(50),
            url_source TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 2. Drugs (One-to-Many)
        CREATE TABLE IF NOT EXISTS eu_trial_drugs (
            id SERIAL PRIMARY KEY,
            eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number) ON DELETE CASCADE,
            drug_name VARCHAR(255),
            active_ingredient VARCHAR(255),
            pharmaceutical_form VARCHAR(255),
            cas_number VARCHAR(50)
        );

        -- 3. Conditions (One-to-Many)
        CREATE TABLE IF NOT EXISTS eu_trial_conditions (
            id SERIAL PRIMARY KEY,
            eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number) ON DELETE CASCADE,
            condition_name TEXT,
            meddra_code VARCHAR(50)
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)
        self.conn.commit()
        logger.info("Schema prepared successfully.")

    def truncate_tables(self) -> None:
        """
        Truncates tables in dependency order (children first then parent) for Full Load.
        """
        if not self.conn:  # pragma: no cover
            self.connect()

        assert self.conn is not None

        # Order: Child tables first, then parent.
        # Or use CASCADE, but explicit is safer.
        sql = "TRUNCATE TABLE eu_trial_drugs, eu_trial_conditions, eu_trials RESTART IDENTITY;"

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
            self.conn.commit()
            logger.info("Truncated Silver tables.")
        except Exception as e:  # pragma: no cover
            self.conn.rollback()
            logger.error(f"Failed to truncate tables: {e}")
            raise

    def bulk_load_stream(self, table_name: str, data_stream: IO[str], columns: List[str]) -> None:
        """
        Uses COPY FROM STDIN to load data efficiently.
        """
        if not self.conn:
            self.connect()

        assert self.conn is not None

        columns_str = ",".join(columns)
        copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER)"

        try:
            with self.conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    while data := data_stream.read(8192):
                        copy.write(data)
            self.conn.commit()
            logger.info(f"Bulk loaded data into {table_name}")
        except Exception as e:  # pragma: no cover
            self.conn.rollback()
            logger.error(f"Failed to bulk load {table_name}: {e}")
            raise

    def upsert_stream(
        self, table_name: str, data_stream: IO[str], columns: List[str], conflict_keys: List[str]
    ) -> None:
        """
        Simulates UPSERT by loading to a temp table then INSERT ON CONFLICT.
        """
        if not self.conn:
            self.connect()

        assert self.conn is not None

        temp_table = f"temp_{table_name}"

        try:
            with self.conn.cursor() as cur:
                # 1. Create Temp Table (Structure like target)
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")

                # 2. Bulk Load to Temp
                columns_str = ",".join(columns)
                copy_sql = f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER)"

                with cur.copy(copy_sql) as copy:
                    while data := data_stream.read(8192):
                        copy.write(data)

                # 2.5 SPECIAL LOGIC: If this is the parent table, clean up children for these IDs
                # This ensures we don't duplicate child records on re-load.
                if table_name == "eu_trials":
                    # Delete from children where eudract_number is in temp table
                    del_sql_drugs = (
                        f"DELETE FROM eu_trial_drugs WHERE eudract_number IN (SELECT eudract_number FROM {temp_table})"
                    )
                    cur.execute(del_sql_drugs)
                    del_sql_conds = (
                        f"DELETE FROM eu_trial_conditions WHERE eudract_number IN "
                        f"(SELECT eudract_number FROM {temp_table})"
                    )
                    cur.execute(del_sql_conds)

                # 3. Merge (Upsert)
                # Construct UPDATE SET clause
                update_cols = [c for c in columns if c not in conflict_keys]
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

                if not update_cols:
                    # Nothing to update (only keys provided?), fallback to DO NOTHING
                    action = "DO NOTHING"
                else:
                    action = f"DO UPDATE SET {update_set}"

                conflict_target = ", ".join(conflict_keys)

                merge_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                SELECT {columns_str} FROM {temp_table}
                ON CONFLICT ({conflict_target}) {action}
                """

                cur.execute(merge_sql)

            self.conn.commit()
            logger.info(f"Upserted data into {table_name}")

        except Exception as e:  # pragma: no cover
            self.conn.rollback()
            logger.error(f"Failed to upsert {table_name}: {e}")
            raise
