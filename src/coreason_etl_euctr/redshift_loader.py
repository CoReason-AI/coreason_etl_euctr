# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import io
import uuid
from typing import IO, Any, List, Optional

import boto3
import psycopg

from coreason_etl_euctr.loader import BaseLoader
from coreason_etl_euctr.logger import logger


class TextToBytesWrapper(io.RawIOBase):
    """
    Wraps a TextIO stream to behave like a BytesIO stream.
    Encodes text to bytes on the fly.
    """

    def __init__(self, text_io: IO[str], encoding: str = "utf-8"):
        self.text_io = text_io
        self.encoding = encoding
        self.buffer = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b: Any) -> int:
        if not self.buffer:
            # Read a chunk from text stream
            chunk = self.text_io.read(8192)
            if not chunk:
                return 0
            self.buffer = chunk.encode(self.encoding)

        length = min(len(b), len(self.buffer))
        b[:length] = self.buffer[:length]
        self.buffer = self.buffer[length:]
        return length


class ChainedStream(io.TextIOBase):
    """
    Helper to chain a string chunk (header) with a remaining stream.
    """

    def __init__(self, first_chunk: str, rest_stream: IO[str]):
        self.first_chunk = first_chunk
        self.rest_stream = rest_stream
        self.pos = 0

    def read(self, size: int | None = -1) -> str:
        # Resolve size to int
        req_size: int = -1 if size is None else size

        if self.pos < len(self.first_chunk):
            available = len(self.first_chunk) - self.pos

            if req_size < 0:
                # Read all
                part1 = self.first_chunk[self.pos :]
                self.pos = len(self.first_chunk)
                return part1 + self.rest_stream.read()

            # Read up to size
            to_read = min(req_size, available)
            part1 = self.first_chunk[self.pos : self.pos + to_read]
            self.pos += to_read

            if to_read == req_size:
                return part1

            # We need more from rest_stream
            remaining = req_size - to_read
            return part1 + self.rest_stream.read(remaining)

        return self.rest_stream.read(req_size)


class RedshiftLoader(BaseLoader):
    """
    AWS Redshift implementation of the BaseLoader.
    Uses 'COPY ... FROM S3' for bulk loading.
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str = "",
        region: Optional[str] = None,
        iam_role: Optional[str] = None,
    ) -> None:
        """
        Initialize the RedshiftLoader.

        Args:
            s3_bucket: The S3 bucket name to use for staging.
            s3_prefix: The prefix for S3 objects.
            region: AWS region.
            iam_role: Optional IAM Role ARN to use for the COPY command.
                      If not provided, attempts to use temporary credentials.
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.region = region
        self.iam_role = iam_role
        self.conn: Optional[psycopg.Connection[Any]] = None
        self.s3_client = boto3.client("s3", region_name=region)

    def connect(self) -> None:
        """
        Connect to Redshift.
        Uses standard PG env vars (PGHOST, etc.) or specific REDSHIFT_* vars if mapped.
        """
        try:
            # Check for REDSHIFT_ specific vars and map them to psycopg args if needed,
            # but simplest is to rely on libpq env vars.
            # However, for clarity, we might want to support explicit Redshift vars.
            # Let's assume standard PG vars are used for the connection itself.
            self.conn = psycopg.connect()
            logger.info("Successfully connected to Redshift.")
        except psycopg.Error as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Connection closed.")

    def prepare_schema(self) -> None:
        """
        Create tables in Redshift.
        Note: Redshift uses VARCHAR(n) similar to Postgres, but doesn't support TEXT[] (arrays) natively
        in the same way (it has SUPER type, or we store as JSON/VARCHAR).
        The FRD Schema uses `age_groups TEXT[]`. Redshift does not support TEXT[].
        We must map `TEXT[]` to `VARCHAR(65535)` (JSON string) or `SUPER`.
        For simplicity and compatibility with the CSV format (which dumps arrays as "{a,b}"),
        we will use `VARCHAR(65535)`.
        """
        if not self.conn:
            raise RuntimeError("Database not connected.")

        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS eu_trials (
                eudract_number VARCHAR(20) PRIMARY KEY,
                sponsor_name VARCHAR(500),
                trial_title VARCHAR(65535),
                start_date DATE,
                trial_status VARCHAR(50),
                age_groups VARCHAR(65535),
                url_source VARCHAR(65535),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS eu_trial_drugs (
                id INT IDENTITY(1,1),
                eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number),
                drug_name VARCHAR(255),
                active_ingredient VARCHAR(255),
                cas_number VARCHAR(50),
                pharmaceutical_form VARCHAR(255),
                PRIMARY KEY (id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS eu_trial_conditions (
                id INT IDENTITY(1,1),
                eudract_number VARCHAR(20) REFERENCES eu_trials(eudract_number),
                condition_name VARCHAR(65535),
                meddra_code VARCHAR(50),
                PRIMARY KEY (id)
            );
            """,
        ]
        # Note: SERIAL -> INT IDENTITY(1,1), TEXT -> VARCHAR(65535) for Redshift best practice (or just VARCHAR).

        try:
            with self.conn.cursor() as cur:
                for sql in ddl_statements:
                    cur.execute(sql)
            self.commit()
            logger.info("Schema preparation complete (Redshift).")
        except psycopg.Error as e:
            self.rollback()
            logger.error(f"Schema preparation failed: {e}")
            raise

    def _upload_to_s3(self, data_stream: IO[str]) -> str:
        """
        Upload the stream to S3 and return the s3:// URI.
        """
        # Generate unique key
        object_key = f"{self.s3_prefix.rstrip('/')}/{uuid.uuid4()}.csv" if self.s3_prefix else f"{uuid.uuid4()}.csv"
        # Remove leading slash if present
        object_key = object_key.lstrip("/")

        logger.info(f"Uploading data to s3://{self.s3_bucket}/{object_key}...")

        # Wrap text stream to bytes
        bytes_stream = TextToBytesWrapper(data_stream)
        self.s3_client.upload_fileobj(bytes_stream, self.s3_bucket, object_key)

        return f"s3://{self.s3_bucket}/{object_key}"

    def _build_copy_command(self, table: str, s3_uri: str, columns_list: Optional[List[str]] = None) -> str:
        """
        Construct the COPY command with credentials.
        """
        creds_clause = ""
        if self.iam_role:
            creds_clause = f"IAM_ROLE '{self.iam_role}'"
        else:
            # Use current session credentials
            session = boto3.Session()
            creds = session.get_credentials()
            if creds:
                frozen = creds.get_frozen_credentials()
                creds_clause = (
                    f"CREDENTIALS 'aws_access_key_id={frozen.access_key};" f"aws_secret_access_key={frozen.secret_key}"
                )
                if frozen.token:
                    creds_clause += f";token={frozen.token}"
                creds_clause += "'"
            else:
                # Fallback, maybe env vars work or machine role?
                # Redshift COPY usually requires explicit credentials or IAM role string.
                # If we can't find them, we might fail.
                # Let's assume user provides IAM role or we have keys.
                # If no creds found, we might raise or try empty (which fails).
                raise ValueError("No IAM Role provided and could not retrieve AWS credentials.")

        cols_part = ""
        if columns_list:
            cols_part = "(" + ", ".join(f'"{c}"' for c in columns_list) + ")"

        # FORMAT CSV IGNOREHEADER 1 (Since Pipeline yields header)
        # DATEFORMAT 'auto' TIMEFORMAT 'auto'
        # NULL AS '' to handle empty strings as NULL? Or empty strings as empty strings?
        # PostgresLoader uses standard CSV. Redshift default CSV is similar.
        # However, Pipeline sends empty string for None.
        return (
            f"COPY {table} {cols_part} FROM '{s3_uri}' "
            f"{creds_clause} "
            f"FORMAT AS CSV IGNOREHEADER 1 "
            f"DATEFORMAT 'auto' TIMEFORMAT 'auto'"
        )

    def bulk_load_stream(self, data_stream: IO[str], target_table: str) -> None:
        if not self.conn:
            raise RuntimeError("Database not connected.")

        # Peek header to get columns
        # We need to read header line without consuming it from the upload stream?
        # TextToBytesWrapper reads from data_stream.
        # If we read from data_stream now, we advance the cursor.
        # Pipeline.stage_data returns a StringIteratorIO.
        # We can't easily peek/seek usually.
        # However, Pipeline yields the header as the FIRST chunk.
        # We can try to read the first line.

        # ISSUE: If we read the header here, we must reconstruct the stream or pass columns explicitly.
        # Easier: Read header line, parse columns, then feed the REST to S3?
        # No, Redshift COPY IGNOREHEADER 1 expects the header in the file.
        # So we should upload the whole stream (header included).
        # But we need columns list for explicit column mapping in COPY (safer).
        # If we rely on matching column order, it's risky.

        # Strategy: Read header line, parse it. Then assume we can chain it back or just use the columns.
        # If data_stream is seekable, we seek(0).
        # If not, we are in trouble if we consume it.
        # StringIteratorIO is not seekable generally (it's a generator wrapper).
        # BUT `Pipeline.stage_data` yields header as first yield.
        # Let's rely on `data_stream` being passed freshly.

        # To get columns WITHOUT consuming:
        # We can't.
        # Modification: We upload the WHOLE stream (including header) to S3.
        # Then we download the first few bytes from S3 to parse header? Too slow.
        # Or we trust the order?
        # Or we accept that `data_stream` might support peeking?

        # Alternative: We read the first chunk, extract header, then prepend it back?
        # `TextToBytesWrapper` can handle a "prefix" buffer.

        # Let's enhance TextToBytesWrapper or handle it here.
        # Reading first line from `data_stream`:
        # `header_line = data_stream.readline()`
        # `columns = header_line.strip().split(',')`
        # Now `data_stream` is advanced.
        # We need to send `header_line` + `data_stream` to S3.

        # Solution: Chain streams.
        header_line = data_stream.readline()
        if not header_line:
            logger.warning(f"Empty stream for {target_table}.")
            return

        columns = [c.strip() for c in header_line.split(",")]

        full_stream = ChainedStream(header_line, data_stream)
        s3_uri = self._upload_to_s3(full_stream)  # type: ignore[arg-type]

        copy_sql = self._build_copy_command(target_table, s3_uri, columns)

        try:
            with self.conn.cursor() as cur:
                logger.info(f"Executing COPY for {target_table} from {s3_uri}...")
                cur.execute(copy_sql)
        except psycopg.Error as e:
            logger.error(f"Redshift COPY failed: {e}")
            raise
        finally:
            self._delete_s3_object(s3_uri)

    def upsert_stream(self, data_stream: IO[str], target_table: str, conflict_keys: List[str]) -> None:
        if not self.conn:
            raise RuntimeError("Database not connected.")

        # 1. Upload to S3 (same logic as bulk_load)
        # We need columns to create temp table or just use LIKE.
        header_line = data_stream.readline()
        if not header_line:
            return
        columns = [c.strip() for c in header_line.split(",")]

        full_stream = ChainedStream(header_line, data_stream)
        s3_uri = self._upload_to_s3(full_stream)  # type: ignore[arg-type]

        # 2. Redshift Upsert Transaction
        staging_table = f"{target_table}_staging_{uuid.uuid4().hex[:8]}"

        try:
            with self.conn.cursor() as cur:
                # Create staging table
                cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {target_table})")

                # COPY to staging
                copy_sql = self._build_copy_command(staging_table, s3_uri, columns)
                cur.execute(copy_sql)

                # DELETE existing rows in target that are in staging
                # USING clause in DELETE is supported in Redshift
                # DELETE FROM target USING staging WHERE target.id = staging.id
                pk_conditions = " AND ".join(f'{target_table}."{k}" = {staging_table}."{k}"' for k in conflict_keys)
                delete_sql = f"DELETE FROM {target_table} USING {staging_table} WHERE {pk_conditions}"
                cur.execute(delete_sql)

                # INSERT new rows
                # We assume columns match
                cols_str = ", ".join(f'"{c}"' for c in columns)
                insert_sql = f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table}"
                cur.execute(insert_sql)

                # Drop staging
                cur.execute(f"DROP TABLE {staging_table}")

            logger.info(f"Redshift Upsert complete for {target_table}.")

        except psycopg.Error as e:
            logger.error(f"Redshift Upsert failed: {e}")
            raise
        finally:
            self._delete_s3_object(s3_uri)

    def truncate_tables(self, table_names: List[str]) -> None:
        if not self.conn:
            raise RuntimeError("Database not connected.")

        # Redshift supports TRUNCATE
        for table in table_names:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"Truncated {table}.")
            except psycopg.Error as e:
                logger.error(f"Failed to truncate {table}: {e}")
                raise

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn:
            self.conn.rollback()

    def _delete_s3_object(self, s3_uri: str) -> None:
        # uri format: s3://bucket/key
        try:
            if s3_uri.startswith("s3://"):
                path = s3_uri[5:]
                bucket, key = path.split("/", 1)
                self.s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            logger.warning(f"Failed to delete S3 object {s3_uri}: {e}")
