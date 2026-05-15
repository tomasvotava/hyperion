"""Concrete schema store adapters.

.. deprecated::
    The abstract :class:`SchemaStore` moved to
    :mod:`hyperion.ports.schema_registry`. Import it from there. This module
    keeps it importable (with a :class:`DeprecationWarning`) for the whole
    ``hyperion-sdk`` 1.x line and still hosts the concrete adapters until S6.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from hyperion._compat import moved_attr
from hyperion.entities.catalog import AssetType
from hyperion.infrastructure.aws import S3Client
from hyperion.log import get_logger
from hyperion.ports.schema_registry import SchemaStore as _SchemaStore

if TYPE_CHECKING:
    from hyperion.ports.schema_registry import SchemaStore

logger = get_logger("schema-store")

AVRO_SCHEMAS_PATH = Path(__file__).parent / "avro_schemas"

_MOVED: dict[str, tuple[object, str]] = {
    "SchemaStore": (_SchemaStore, "hyperion.ports.schema_registry"),
}

__all__ = [
    "AVRO_SCHEMAS_PATH",
    "LocalSchemaStore",
    "S3SchemaStore",
    "SchemaStore",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        value, new_module = _MOVED[name]
        return moved_attr(
            name=name, value=value, old_module="hyperion.catalog.schema", new_module=new_module
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class LocalSchemaStore(_SchemaStore):
    """Schema store for local files."""

    def __init__(self, schemas_path: Path = AVRO_SCHEMAS_PATH) -> None:
        """Initialize the local schema store with the given path.

        Args:
            schemas_path (Path, optional): The path to the schemas. Defaults to AVRO_SCHEMAS_PATH.
        """
        super().__init__(schemas_path.as_posix())
        self.schemas_path = schemas_path
        if not schemas_path.exists():
            logger.critical("Provided schemas path does not exist.", schemas_path=schemas_path.as_posix())
            raise FileNotFoundError("Provided schemas path does not exist.")

    def get_schema_from_path(self, schema_path: str) -> dict[str, Any]:
        path = Path(schema_path)
        logger.info("Reading avro schema from stored json file.", path=path.as_posix())
        with path.open("r", encoding="utf-8") as file:
            schema = json.load(file)
        if not isinstance(schema, dict):
            raise TypeError(f"Schema has unexpected type {type(schema)}, expected 'dict'.")
        return schema

    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        path = self.schemas_path / asset_type / f"{asset_name}.v{schema_version}.avro.json"
        logger.info(
            "Reading avro schema from stored json file.",
            path=path.as_posix(),
            asset_name=asset_name,
            asset_type=asset_type,
        )
        return self.get_schema_from_path(path.as_posix())


@lru_cache(maxsize=256)
def _get_schema_from_s3(bucket: str, key: str, client: S3Client) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(client.download_as_string(bucket, key)))


class S3SchemaStore(_SchemaStore):
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
