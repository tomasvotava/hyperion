"""S3-backed :class:`SchemaStore` adapter (requires boto3 -- ``[aws]``)."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, cast

from hyperion.domain.assets import AssetType
from hyperion.infrastructure.aws import S3Client
from hyperion.log import get_logger
from hyperion.ports.schema_registry import SchemaStore

logger = get_logger("schema-store")


@lru_cache(maxsize=256)
def _get_schema_from_s3(bucket: str, key: str, client: S3Client) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(client.download_as_string(bucket, key)))


class S3SchemaStore(SchemaStore):
    """Schema store for S3."""

    def __init__(self, bucket: str, prefix: str) -> None:
        """Initialize the S3 schema store with the given bucket and prefix.

        Args:
            bucket (str): The S3 bucket.
            prefix (str): The prefix in the bucket.
        """
        super().__init__(f"s3://{bucket}/{prefix}")
        self.bucket = bucket
        self.prefix = prefix
        self.s3_client = S3Client()

    def get_schema_from_path(self, schema_path: str) -> dict[str, Any]:
        logger.info("Getting avro schema from S3.", bucket=self.bucket, key=schema_path)
        try:
            return _get_schema_from_s3(self.bucket, schema_path, self.s3_client)
        except Exception:
            logger.critical("Failed to get avro schema from S3.", bucket=self.bucket, key=schema_path)
            raise

    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        key = f"{asset_type}/{asset_name}.v{schema_version}.avro.json"
        return self.get_schema_from_path(key)
