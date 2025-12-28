# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, cast

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]
    ClientError = None  # type: ignore[assignment]


class StorageBackend(ABC):
    """
    Abstract Base Class for storage backends (Local vs S3).
    """

    @abstractmethod
    def write(self, key: str, content: str) -> None:
        """
        Write string content to the storage at the given key (filename).
        """
        pass  # pragma: no cover

    @abstractmethod
    def read(self, key: str) -> str:
        """
        Read string content from the storage.
        Raises FileNotFoundError (or equivalent) if not found.
        """
        pass  # pragma: no cover

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if the key exists in storage.
        """
        pass  # pragma: no cover


class LocalStorageBackend(StorageBackend):
    """
    Implementation of StorageBackend for the local filesystem.
    """

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(self, key: str, content: str) -> None:
        file_path = self.base_path / key
        # Ensure parent dir exists (though keys are usually flat filenames in this project)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")

    def read(self, key: str) -> str:
        file_path = self.base_path / key
        if not file_path.exists():
            raise FileNotFoundError(f"Key {key} not found in {self.base_path}")
        return file_path.read_text(encoding="utf-8")

    def exists(self, key: str) -> bool:
        return (self.base_path / key).exists()


class S3StorageBackend(StorageBackend):
    """
    Implementation of StorageBackend for AWS S3.
    """

    def __init__(self, bucket_name: str, prefix: str = "", region_name: Optional[str] = None):
        if boto3 is None:  # pragma: no cover
            raise ImportError("boto3 is required for S3StorageBackend")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.client = boto3.client("s3", region_name=region_name)

    def _get_full_key(self, key: str) -> str:
        # Join prefix and key, avoiding double slashes if prefix is empty
        if not self.prefix:
            return key
        return f"{self.prefix.rstrip('/')}/{key.lstrip('/')}"

    def write(self, key: str, content: str) -> None:
        full_key = self._get_full_key(key)
        self.client.put_object(
            Bucket=self.bucket_name, Key=full_key, Body=content.encode("utf-8"), ContentType="text/html"
        )

    def read(self, key: str) -> str:
        full_key = self._get_full_key(key)
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=full_key)
            body = cast(bytes, response["Body"].read())
            return body.decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"Key {full_key} not found in bucket {self.bucket_name}") from e
            raise

    def exists(self, key: str) -> bool:
        full_key = self._get_full_key(key)
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=full_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            # For some S3 compatible stores or permissions issues, it might be 403, but 404 is standard for missing.
            # boto3 head_object raises 404 for missing keys.
            return False
