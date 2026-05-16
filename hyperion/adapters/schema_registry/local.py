"""Local-filesystem :class:`SchemaStore` adapter (lite -- stdlib only)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hyperion.domain.assets import AssetType
from hyperion.log import get_logger
from hyperion.ports.schema_registry import SchemaStore

logger = get_logger("schema-store")

# Canonical location of the packaged avro schemas: ``hyperion/catalog/avro_schemas``.
# ``parents[2]`` of ``hyperion/adapters/schema_registry/local.py`` is the ``hyperion``
# package root. Preserved verbatim from the pre-S6 ``hyperion.catalog.schema`` default.
AVRO_SCHEMAS_PATH = Path(__file__).resolve().parents[2] / "catalog" / "avro_schemas"


class LocalSchemaStore(SchemaStore):
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
