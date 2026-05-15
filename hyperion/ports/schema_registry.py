"""Port: schema store abstraction.

Abstract :class:`SchemaStore` base. Concrete adapters (``LocalSchemaStore``,
``S3SchemaStore``) live in ``hyperion.catalog.schema`` until step S6;
``_create_new`` reaches them via a deferred import. ``AssetProtocol`` /
``AssetType`` are referenced only in annotations, so this port stays free of
the pandera/polars data stack.
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from hyperion.config import storage_config
from hyperion.log import get_logger

if TYPE_CHECKING:
    from hyperion.domain.assets import AssetProtocol, AssetType

logger = get_logger("schema-store")


class SchemaStore(abc.ABC):
    """Abstract base class for schema stores."""

    _instances: ClassVar[dict[str, SchemaStore]] = {}

    def __init__(self, path: str) -> None:
        """Initialize the schema store with the given path.

        Args:
            path (str): The path to the schema store.
        """
        self.path = path

    def get_asset_schema(self, asset: AssetProtocol) -> dict[str, Any]:
        """Get the schema for the given asset.

        Args:
            asset (AssetProtocol): The asset to get the schema for.

        Returns:
            dict[str, Any]: The schema for the asset.
        """
        return self.get_schema(asset.name, asset.schema_version, asset_type=asset.asset_type)

    @abc.abstractmethod
    def get_schema_from_path(self, schema_path: str) -> dict[str, Any]:
        """Get the schema given by its path.

        Args:
            schema_path (str): The path to the schema.

        Returns:
            dict[str, Any]: The schema.
        """

    @abc.abstractmethod
    def get_schema(self, asset_name: str, schema_version: int, asset_type: AssetType) -> dict[str, Any]:
        """Get the schema for the asset with the given name and version."""

    @staticmethod
    def _create_new(path: str) -> SchemaStore:
        from hyperion.catalog.schema import LocalSchemaStore, S3SchemaStore

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
    def from_path(path: str) -> SchemaStore:
        """Get a schema store from the given path.

        Args:
            path (str): The path to the schema store.

        Returns:
            SchemaStore: The schema store.
        """
        if path not in SchemaStore._instances:
            SchemaStore._instances[path] = SchemaStore._create_new(path)
        return SchemaStore._instances[path]

    @staticmethod
    def from_config() -> SchemaStore:
        """Get a schema store from the configuration.

        Returns:
            SchemaStore: The schema store.
        """
        return SchemaStore.from_path(storage_config.schema_path)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} path={self.path!r}>"
