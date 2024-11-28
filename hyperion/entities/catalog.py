import datetime
import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from hyperion.catalog.schema import SchemaStore
from hyperion.dateutils import TimeResolution, assure_timezone


def _get_prefixed_path(path: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        prefix = f"{prefix}/"
    return prefix + path


class AssetProtocol(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def schema_version(self) -> int: ...

    def get_schema(self) -> dict[str, Any]: ...

    def get_path(self, prefix: str = "") -> str: ...

    def to_metadata(self) -> dict[str, str]: ...


@dataclass(frozen=True, eq=True)
class DataLakeAsset:
    name: str
    date: datetime.datetime
    schema_version: int = 1

    def get_schema(self) -> dict[str, Any]:
        return SchemaStore.from_config().get_schema(self.name, self.schema_version, "data_lake")

    def get_path(self, prefix: str = "") -> str:
        date = assure_timezone(self.date).isoformat()
        return _get_prefixed_path(f"{self.name}/date={date}/v{self.schema_version}.avro", prefix)

    def to_metadata(self) -> dict[str, str]:
        return {"name": self.name, "date": self.date.isoformat(), "schema_version": str(self.schema_version)}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"


@dataclass(frozen=True, eq=True)
class PersistentStoreAsset:
    name: str
    schema_version: int = 1

    def get_schema(self) -> dict[str, Any]:
        return SchemaStore.from_config().get_schema(self.name, self.schema_version, "persistent_store")

    def get_path(self, prefix: str = "") -> str:
        return _get_prefixed_path(f"{self.name}/v{self.schema_version}.avro", prefix)

    def to_metadata(self) -> dict[str, str]:
        return {"name": self.name, "schema_version": str(self.schema_version)}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"


@dataclass(frozen=True, eq=True)
class FeatureAsset:
    name: str
    timestamp: datetime.datetime
    resolution: TimeResolution | str
    schema_version: int = 1
    partition_keys: dict[str, str] = field(default_factory=dict)

    @property
    def time_resolution(self) -> TimeResolution:
        if isinstance(self.resolution, TimeResolution):
            return self.resolution
        return TimeResolution.from_str(self.resolution)

    @property
    def feature_name(self) -> str:
        return f"{self.name}.{self.time_resolution!r}"

    def _get_partition_keys_prefix(self) -> str:
        key_names = sorted(self.partition_keys.keys())
        return ("/".join(f"{key}={self.partition_keys[key]}" for key in key_names)).strip("/")

    def get_schema(self) -> dict[str, Any]:
        return SchemaStore.from_config().get_schema(self.name, self.schema_version, "feature")

    def get_path(self, prefix: str = "") -> str:
        timestamp = assure_timezone(self.timestamp).isoformat()
        keys_prefix = self._get_partition_keys_prefix()
        if keys_prefix:
            keys_prefix = keys_prefix + "/"
        return _get_prefixed_path(
            f"{self.feature_name}/{keys_prefix}timestamp={timestamp}/v{self.schema_version}.avro", prefix
        )

    def to_metadata(self) -> dict[str, str]:
        return {
            "name": self.name,
            "timestamp": self.timestamp.isoformat(),
            "schema_version": str(self.schema_version),
            "partition_keys": json.dumps(self.partition_keys),
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"
