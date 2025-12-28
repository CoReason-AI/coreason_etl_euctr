# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import random
import string
from typing import IO, Any, List, Optional

import psycopg
from loguru import logger

from coreason_etl_euctr.loader import BaseLoader


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
                pharmaceutical_form VARCHAR(255),
                CONSTRAINT uq_trial_drug UNIQUE (eudract_number, drug_name, pharmaceutical_form)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS eu_trial_conditions (
                id SERIAL PRIMARY KEY,
                eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number),
                condition_name TEXT,
                meddra_code VARCHAR(50),
                CONSTRAINT uq_trial_condition UNIQUE (eudract_number, condition_name)
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
        Reads the first line (header) to determine columns, then uses explicit column list.
        """
        if not self.conn:
            raise RuntimeError("Database not connected.")

        # Read header to determine columns
        # Note: We assume the stream yields the header as the first chunk (from Pipeline)
        # or we read until newline.
        header_chunk = data_stream.read()
        if not header_chunk:
            logger.warning(f"Empty stream for {target_table}, skipping.")
            return

        # Extract header line
        if "\n" in header_chunk:
            header_line, remaining_chunk = header_chunk.split("\n", 1)
        else:
            header_line = header_chunk
            remaining_chunk = ""

        # Parse columns (simple CSV split, assuming no commas in headers)
        columns_list = [c.strip() for c in header_line.split(",")]
        # Quote columns to handle reserved words/special chars
        columns_str = ", ".join(f'"{c}"' for c in columns_list)

        # Use explicit columns in COPY to handle schema mismatch (e.g. auto-id in DB)
        # We removed the header from the stream, so we don't use 'HEADER' option.
        sql = f"COPY {target_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV)"

        try:
            with self.conn.cursor() as cur:
                with cur.copy(sql) as copy:
                    # Write the remaining part of the first chunk
                    if remaining_chunk:
                        copy.write(remaining_chunk)
                    # Write rest of stream
                    while data := data_stream.read(8192):
                        copy.write(data)
            logger.info(f"Bulk loaded data into {target_table}.")
        except psycopg.Error as e:
            logger.error(f"Bulk load failed for {target_table}: {e}")
            raise

    def upsert_stream(self, data_stream: IO[str], target_table: str, conflict_keys: List[str]) -> None:
        """
        Execute UPSERT via staging table.
        1. Create temp table (structure copied from target).
        2. COPY data to temp table.
        3. INSERT ... ON CONFLICT DO UPDATE from temp table to target.
        """
        if not self.conn:
            raise RuntimeError("Database not connected.")

        if not conflict_keys:
            raise ValueError("Conflict keys required for upsert.")

        # Generate a unique temp table name to avoid collisions in concurrent sessions (though usually session local)
        # Using a random suffix just to be safe if multiple calls happen in same session/transaction
        # context if not dropped.
        suffix = "".join(random.choices(string.ascii_lowercase, k=6))
        temp_table = f"{target_table}_staging_{suffix}"

        # Read header to determine columns
        header_chunk = data_stream.read()
        if not header_chunk:
            logger.warning(f"Empty stream for {target_table}, skipping upsert.")
            return

        if "\n" in header_chunk:
            header_line, remaining_chunk = header_chunk.split("\n", 1)
        else:
            header_line = header_chunk
            remaining_chunk = ""

        columns_list = [c.strip() for c in header_line.split(",")]
        # Quote columns to handle reserved words/special chars
        columns_str = ", ".join(f'"{c}"' for c in columns_list)

        try:
            with self.conn.cursor() as cur:
                # 1. Create Temp Table (Structure only)
                # "CREATE TEMP TABLE ... LIKE ... INCLUDING ALL" copies structure.
                # However, Postgres temp tables are session-local.
                logger.debug(f"Creating temp table {temp_table}...")
                create_temp_sql = (
                    f"CREATE TEMP TABLE {temp_table} " f"(LIKE {target_table} INCLUDING ALL) " "ON COMMIT DROP"
                )
                cur.execute(create_temp_sql)

                # 2. Bulk Load to Temp Table
                # Use explicit columns
                sql_copy = f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV)"
                with cur.copy(sql_copy) as copy:
                    if remaining_chunk:
                        copy.write(remaining_chunk)
                    while data := data_stream.read(8192):
                        copy.write(data)

                # 3. Perform Upsert
                # Retrieve column names to construct the UPDATE clause
                # We query information_schema or just select * from temp limit 0 to get description?
                # Faster: use the cursor description from a dummy select or query system catalogs.
                # Let's query columns from the temp table (since it exists now).
                cur.execute(f"SELECT * FROM {temp_table} LIMIT 0")
                if not cur.description:
                    logger.warning(f"No columns found in {temp_table}, skipping upsert.")
                    return

                columns = [desc.name for desc in cur.description]
                # Quote columns in SELECT/INSERT list
                cols_str = ", ".join(f'"{c}"' for c in columns)

                # Build SET clause: "col" = EXCLUDED."col" for all non-key columns
                # We exclude conflict keys from the update set usually, or update them too (idempotent).
                update_assignments = [f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in conflict_keys]

                # If there are no columns to update (e.g. table only has PKs?), we do NOTHING?
                # But requirement says "ON CONFLICT UPDATE".
                # If update_assignments is empty (only PKs), we might want DO NOTHING.
                # But assume there are other columns.

                # Conflict target also needs quoting if keys are reserved words, but typically user provides clean keys.
                # To be safe, we can quote them too, but user passes them as list of strings which might be raw.
                # However, keys are usually column names.
                conflict_target = ", ".join(f'"{k}"' for k in conflict_keys)

                if update_assignments:
                    update_clause = f"UPDATE SET {', '.join(update_assignments)}"
                    action = update_clause
                else:
                    action = "NOTHING"

                insert_sql = (
                    f"INSERT INTO {target_table} ({cols_str}) "
                    f"SELECT {cols_str} FROM {temp_table} "
                    f"ON CONFLICT ({conflict_target}) DO {action}"
                )

                logger.debug(f"Executing Upsert on {target_table}...")
                cur.execute(insert_sql)

                # Temp table is ON COMMIT DROP, but we can drop it explicitly to save resources if transaction is long
                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

            logger.info(f"Upsert complete for {target_table}.")

        except psycopg.Error as e:
            logger.error(f"Upsert failed for {target_table}: {e}")
            raise

    def truncate_tables(self, table_names: List[str]) -> None:
        """
        Truncate the specified tables using TRUNCATE TABLE ... CASCADE.
        """
        if not self.conn:
            raise RuntimeError("Database not connected.")

        if not table_names:
            return

        tables_str = ", ".join(table_names)
        sql = f"TRUNCATE TABLE {tables_str} CASCADE"

        try:
            with self.conn.cursor() as cur:
                logger.debug(f"Truncating tables: {tables_str}")
                cur.execute(sql)
            logger.info(f"Truncated tables: {tables_str}")
        except psycopg.Error as e:
            logger.error(f"Failed to truncate tables {tables_str}: {e}")
            raise

    def commit(self) -> None:
        """Commit transaction."""
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        """Rollback transaction."""
        if self.conn:
            self.conn.rollback()
