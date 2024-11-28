"""Schema store."""

import abc
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, ClassVar, Literal, cast
from urllib.parse import urlparse

from hyperion.config import storage_config
from hyperion.infrastructure.aws import S3Client
from hyperion.logging import get_logger

logger = get_logger("schema-store")

AVRO_SCHEMAS_PATH = Path(__file__).parent / "avro_schemas"

AssetType = Literal["data_lake", "feature", "persistent_store"]


class SchemaStore(abc.ABC):
    _instances: ClassVar[dict[str, "SchemaStore"]] = {}

    def __init__(self, path: str) -> None:
        self.path = path

    @abc.abstractmethod
    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        """Get the schema for the asset with the given name and version."""

    @staticmethod
    def _create_new(path: str) -> "SchemaStore":
        parsed = urlparse(path)
        if parsed.scheme == "file" or not parsed.scheme:
            resolved = (Path(parsed.netloc or "/") / parsed.path.lstrip("/")).resolve()
            logger.info("Using file schema store.", path=resolved.as_posix())
            return LocalSchemaStore(resolved)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")
            logger.info("Using S3 schema store.", bucket=bucket, prefix=prefix)
            return S3SchemaStore(bucket, prefix)
        logger.critical("Unsupported schema store scheme.", scheme=parsed.scheme, path=storage_config.schema_path)
        raise ValueError(f"Unsupported schema store scheme {parsed.scheme!r}.")

    @staticmethod
    def from_path(path: str) -> "SchemaStore":
        if path not in SchemaStore._instances:
            SchemaStore._instances[path] = SchemaStore._create_new(path)
        return SchemaStore._instances[path]

    @staticmethod
    def from_config() -> "SchemaStore":
        return SchemaStore.from_path(storage_config.schema_path)


class LocalSchemaStore(SchemaStore):
    def __init__(self, schemas_path: Path = AVRO_SCHEMAS_PATH) -> None:
        super().__init__(schemas_path.as_posix())
        self.schemas_path = schemas_path
        if not schemas_path.exists():
            logger.critical("Provided schemas path does not exist.", schemas_path=schemas_path.as_posix())
            raise FileNotFoundError("Provided schemas path does not exist.")

    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        path = self.schemas_path / asset_type / f"{asset_name}.v{schema_version}.avro.json"
        logger.info(
            "Reading avro schema from stored json file.",
            path=path.as_posix(),
            asset_name=asset_name,
            asset_type=asset_type,
        )
        try:
            with path.open("r", encoding="utf-8") as file:
                schema = json.load(file)
            if not isinstance(schema, dict):
                raise TypeError(f"Schema has unexpected type {type(schema)}, expected 'dict'.")
            return schema
        except Exception:
            logger.critical(
                "Failed to get avro schema from stored json file.",
                path=path.as_posix(),
                asset_name=asset_name,
                asset_type=asset_type,
            )
            raise


@lru_cache(maxsize=256)
def _get_schema_from_s3(bucket: str, key: str, client: S3Client) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(client.download_as_string(bucket, key)))


class S3SchemaStore(SchemaStore):
    def __init__(self, bucket: str, prefix: str) -> None:
        super().__init__(f"s3://{bucket}/{prefix}")
        self.bucket = bucket
        self.prefix = prefix
        self.s3_client = S3Client()

    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        key = f"{asset_type}/{asset_name}.v{schema_version}.avro.json"
        logger.info("Getting avro schema from S3.", bucket=self.bucket, key=key)
        try:
            return _get_schema_from_s3(self.bucket, key, self.s3_client)
        except Exception:
            logger.critical("Failed to get avro schema from S3.", bucket=self.bucket, key=key)
            raise
