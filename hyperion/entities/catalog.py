import datetime
import json
from dataclasses import dataclass, field
from typing import ClassVar, Literal, Protocol

from hyperion.dateutils import TimeResolution, assure_timezone

AssetType = Literal["data_lake", "feature", "persistent_store"]


def _get_prefixed_path(path: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        prefix = f"{prefix}/"
    return prefix + path


class AssetProtocol(Protocol):
    """Protocol for assets in the catalog.

    This protocol defines the interface for assets in the catalog.

    Attributes:
        asset_type (ClassVar[AssetType]): The type of the asset.
        name (str): The name of the asset.
        schema_version (int): The schema version of the asset.
    """

    asset_type: ClassVar[AssetType]

    @property
    def name(self) -> str: ...

    @property
    def schema_version(self) -> int:
        """The schema version of the asset."""

    def get_path(self, prefix: str = "") -> str:
        """Get the path for the asset with the given prefix.

        Args:
            prefix (str): The prefix for the path.

        Returns:
            str: The path for the asset.
        """

    def to_metadata(self) -> dict[str, str]:
        """Get the metadata for the asset.

        Returns:
            dict[str, str]: The metadata for the asset.
        """


@dataclass(frozen=True, eq=True)
class DataLakeAsset:
    """Data lake asset.

    Attributes:
        asset_type (ClassVar[AssetType]): The type of the asset.
        name (str): The name of the asset.
        date (datetime.datetime): The date of the asset.
        schema_version (int): The schema version of the asset.
    """

    asset_type: ClassVar[AssetType] = "data_lake"
    name: str
    date: datetime.datetime
    schema_version: int = 1

    def get_path(self, prefix: str = "") -> str:
        """Get the path for the asset with the given prefix."""
        date = assure_timezone(self.date).isoformat()
        return _get_prefixed_path(f"{self.name}/date={date}/v{self.schema_version}.avro", prefix)

    def to_metadata(self) -> dict[str, str]:
        """Get the metadata for the asset."""
        return {"name": self.name, "date": self.date.isoformat(), "schema_version": str(self.schema_version)}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"


@dataclass(frozen=True, eq=True)
class PersistentStoreAsset:
    """Persistent store asset.

    Attributes:
        asset_type (ClassVar[AssetType]): The type of the asset.
        name (str): The name of the asset.
        schema_version (int): The schema version of the asset.
    """

    asset_type: ClassVar[AssetType] = "persistent_store"
    name: str
    schema_version: int = 1

    def get_path(self, prefix: str = "") -> str:
        """Get the path for the asset with the given prefix."""
        return _get_prefixed_path(f"{self.name}/v{self.schema_version}.avro", prefix)

    def to_metadata(self) -> dict[str, str]:
        """Get the metadata for the asset."""
        return {"name": self.name, "schema_version": str(self.schema_version)}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"


@dataclass(frozen=True, eq=True)
class FeatureAsset:
    """Feature asset.

    Attributes:
        asset_type (ClassVar[AssetType]): The type of the asset.
        name (str): The name of the asset.
        timestamp (datetime.datetime): The timestamp of the asset.
        resolution (TimeResolution | str): The resolution of the asset.
        schema_version (int): The schema version of the asset.
        partition_keys (dict[str, str]): The partition keys of the asset.
    """

    asset_type: ClassVar[AssetType] = "feature"
    name: str
    timestamp: datetime.datetime
    resolution: TimeResolution | str
    schema_version: int = 1
    partition_keys: dict[str, str] = field(default_factory=dict)

    @property
    def time_resolution(self) -> TimeResolution:
        """The time resolution of the feature."""
        if isinstance(self.resolution, TimeResolution):
            return self.resolution
        return TimeResolution.from_str(self.resolution)

    @property
    def feature_name(self) -> str:
        """The name of the feature including the time resolution."""
        return f"{self.name}.{self.time_resolution!r}"

    def _get_partition_keys_prefix(self) -> str:
        key_names = sorted(self.partition_keys.keys())
        return ("/".join(f"{key}={self.partition_keys[key]}" for key in key_names)).strip("/")

    def get_path(self, prefix: str = "") -> str:
        """Get the path for the asset with the given prefix."""
        timestamp = assure_timezone(self.timestamp).isoformat()
        keys_prefix = self._get_partition_keys_prefix()
        if keys_prefix:
            keys_prefix = keys_prefix + "/"
        return _get_prefixed_path(
            f"{self.feature_name}/{keys_prefix}timestamp={timestamp}/v{self.schema_version}.avro", prefix
        )

    def to_metadata(self) -> dict[str, str]:
        """Get the metadata for the asset."""
        return {
            "name": self.name,
            "timestamp": self.timestamp.isoformat(),
            "schema_version": str(self.schema_version),
            "partition_keys": json.dumps(self.partition_keys),
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.get_path()!r})"
