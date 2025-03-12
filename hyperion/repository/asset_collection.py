"""Asset collection is a class that allows you to fetch
and store data from the catalog in a type-safe manner.
"""

import asyncio
import datetime
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any, ClassVar, Generic, TypeVar, cast

from hyperion.asyncutils import iter_async
from hyperion.catalog import Catalog
from hyperion.dateutils import utcnow
from hyperion.entities.catalog import FeatureAsset, FeatureModel
from hyperion.log import get_logger
from hyperion.typeutils import DateOrDelta

logger = get_logger("hyperion-model-specification")

CClass = TypeVar("CClass", bound=FeatureModel)
CollectionType = TypeVar("CollectionType", bound="AssetCollection")


@dataclass(frozen=True, eq=True)
class FeatureAssetSpecification(Generic[CClass]):
    """Specification for fetching feature assets from the catalog.

    Args:
        feature: The feature model class to fetch.
        start_date: The start date or delta from now to fetch the data.
        end_date: The end date or delta from now to fetch the data.
    """

    feature: type[CClass]
    start_date: DateOrDelta | None = None
    end_date: DateOrDelta | None = None

    @staticmethod
    def _resolve_date(
        date_spec: DateOrDelta | None, default: datetime.datetime, the_now: datetime.datetime | None
    ) -> datetime.datetime:
        if date_spec is None:
            return default
        if isinstance(date_spec, datetime.datetime):
            return date_spec
        return (the_now or utcnow()) + date_spec

    def resolve_start_date(self, the_now: datetime.datetime | None = None) -> datetime.datetime:
        """Resolve the start date for fetching the feature asset data."""
        return self._resolve_date(self.start_date, datetime.datetime.min, the_now)

    def resolve_end_date(self, the_now: datetime.datetime | None = None) -> datetime.datetime:
        """Resolve the end date for fetching the feature asset data."""
        return self._resolve_date(self.end_date, utcnow(), the_now)


@dataclass
class _CollectionState:
    """Internal state of the asset collection.

    Args:
        fetched: Whether the data has been fetched.
        data: A mapping of field names to the fetched data.
        fetch_specifications: A mapping of field names to the fetch specifications.
        anchor_timestamps: The anchor timestamps for fetching the data.
        semaphore: The asyncio semaphore for controlling concurrency.
        max_concurrency: The maximum concurrency for fetching data.
    """

    fetched: bool = False
    data: dict[str, list[Any]] = field(default_factory=dict)
    fetch_specifications: dict[str, FeatureAssetSpecification[Any]] = field(default_factory=dict)
    anchor_timestamps: dict[str, datetime.datetime | None] = field(default_factory=dict)
    semaphore: asyncio.Semaphore | None = None
    max_concurrency: int | None = None


class AssetCollection:
    """A collection of feature assets that can be fetched from the catalog.

    Attributes:
        catalog: The catalog to fetch the data from. If not set, it will be created from the config.
        max_concurrency: The maximum concurrency for fetching data. Default is 8.
        reserved_fields: The reserved field names for the collection.
        _state: The internal state of the collection. It should under no circumstances be modified directly.
    """

    catalog: ClassVar[Catalog | None] = None
    max_concurrency: ClassVar[int] = 8
    reserved_fields: ClassVar = ("catalog", "max_concurrency", "reserved_fields")
    _state: ClassVar[_CollectionState]

    @classmethod
    def _get_state(cls) -> _CollectionState:
        if not hasattr(cls, "_state"):
            logger.debug("Creating new empty state for the collection.", collection=cls.__name__)
            cls._state = _CollectionState()
        return cls._state

    @classmethod
    def _get_semaphore(cls) -> asyncio.Semaphore:
        state = cls._get_state()
        if state.semaphore is None:
            logger.debug("Creating new semaphore.", collection=cls.__name__, max_concurrency=cls.max_concurrency)
            state.semaphore = asyncio.Semaphore(cls.max_concurrency)
            state.max_concurrency = cls.max_concurrency
        if state.max_concurrency != cls.max_concurrency:
            logger.warning(
                "Config max_concurrency cannot be changed after first use of the collection.", collection=cls.__name__
            )
        return state.semaphore

    @classmethod
    def is_fetched(cls) -> bool:
        """Check if the collection has fetched all data."""
        return cls._get_state().fetched

    @classmethod
    def get_data(cls, field: str) -> list[Any]:
        """Get the fetched data for the given field."""
        state = cls._get_state()
        if field not in state.data:
            raise ValueError(f"Data for {field!r} has not been fetched yet. Did you call 'fetch_all()'?")
        return state.data[field]

    @classmethod
    def clear(cls) -> None:
        """Clear all fetched data from the collection."""
        logger.info("Clearing all fetched data from the collection.", collection=cls.__name__)
        cls._get_state().data = {}
        cls._get_state().fetched = False

    @classmethod
    def _get_catalog(cls) -> Catalog:
        if cls.catalog is None:
            cls.catalog = Catalog.from_config()
        return cls.catalog

    @classmethod
    async def _gather_asset_range(
        cls, asset_spec: FeatureAssetSpecification[CClass], the_now: datetime.datetime
    ) -> list[CClass]:
        start_date = asset_spec.resolve_start_date(the_now)
        end_date = asset_spec.resolve_end_date(the_now)
        partitions = list(
            cls._get_catalog().iter_feature_store_partitions(
                asset_spec.feature.asset_name,
                asset_spec.feature.resolution,
                start_date,
                end_date,
                asset_spec.feature.schema_version,
            )
        )
        all_data: list[CClass] = []

        async def _retrieve_async(partition: FeatureAsset) -> list[dict[str, Any]]:
            async with cls._get_semaphore():
                logger.debug("Retrieving partition.", partition=partition)
                return await asyncio.to_thread(list, cls._get_catalog().retrieve_asset(partition))

        tasks: list[Coroutine[None, None, list[dict[str, Any]]]] = []

        logger.info(
            f"Retrieving {len(partitions)} partitions.",
            partitions=len(partitions),
            asset_name=asset_spec.feature.asset_name,
        )
        for partition in partitions:
            tasks.append(_retrieve_async(partition))
        results = await asyncio.gather(*tasks)
        for data in results:
            all_data.extend(asset_spec.feature(**row) for row in data)
        logger.info(
            f"Downloaded {len(all_data)} rows from {len(partitions)} partitions.",
            asset_name=asset_spec.feature.asset_name,
        )
        return all_data

    @classmethod
    def register_specification(
        cls,
        field_name: str,
        specification: FeatureAssetSpecification[CClass],
        anchor_timestamp: datetime.datetime | None = None,
    ) -> None:
        """Register a fetch specification for a field in the collection.

        This is normally only called by the `FeatureFetchSpecifier` descriptor and should
        not be called directly.

        Args:
            field_name: The name of the field to register the specification for.
            specification: The fetch specification for the field.
            anchor_timestamp: The anchor timestamp for fetching the data.
        """
        if field_name in cls._get_state().fetch_specifications:
            logger.warning(
                "Registering duplicate fetch specification, existing will be discarded.",
                field=field_name,
                asset_name=specification.feature.asset_name,
            )
        logger.debug("Registering field into an asset collection.", collection=cls.__name__, field=field_name)
        cls._get_state().fetch_specifications[field_name] = specification
        cls._get_state().anchor_timestamps[field_name] = anchor_timestamp

    @classmethod
    async def fetch_all(cls) -> None:
        """Fetch all data for the collection."""
        if cls.is_fetched():
            logger.info(
                "Collection already fetched all data, if you want to start over, call .clear()", collection=cls.__name__
            )
            return
        logger.info("Gather all data within the collection.", collection=cls.__name__)
        tasks: list[Coroutine[None, None, tuple[str, list[Any]]]] = []

        async def _gather(name: str, specs: FeatureAssetSpecification[CClass]) -> tuple[str, list[CClass]]:
            anchor_timestamp = cls._get_state().anchor_timestamps.get(name) or utcnow()
            return (name, await cls._gather_asset_range(specs, anchor_timestamp))

        async for prop, specs in iter_async(cls._get_state().fetch_specifications.items()):
            tasks.append(_gather(prop, specs))

        results = await asyncio.gather(*tasks)
        for name, data in results:
            logger.info("Finished receiving feature data.", field=name)
            cls._get_state().data[name] = data

        cls._get_state().fetched = True


class _FeatureFetchSpecifier(Generic[CClass]):
    def __init__(
        self,
        feature: type[CClass],
        start_date: DateOrDelta | None = None,
        end_date: DateOrDelta | None = None,
    ) -> None:
        self._specification = FeatureAssetSpecification(feature, start_date, end_date)
        self._owner: type[AssetCollection] | None = None
        self._field_name: str | None = None

    @property
    def owner(self) -> type[AssetCollection]:
        if self._owner is None:
            raise RuntimeError("Field was not properly initialized and has no owner.")
        return self._owner

    @property
    def field_name(self) -> str:
        if self._field_name is None:
            raise RuntimeError("Field was not properly initialized and has no name.")
        return self._field_name

    def __set_name__(self, owner: type[AssetCollection], field_name: str) -> None:
        if not issubclass(owner, AssetCollection) and owner is not AssetCollection:
            raise TypeError(
                f"{self.__class__.__name__!r} can only be a field of AssetCollection or its subclass, "
                f"{owner!r} is not a valid owner."
            )
        if field_name.startswith("_") or field_name in owner.reserved_fields:
            raise ValueError(f"Field name {field_name!r} is reserved for internal use.")
        self._owner = owner
        self._field_name = field_name
        owner.register_specification(self.field_name, self._specification)

    def __get__(self, _instance: AssetCollection, _instance_type: type[AssetCollection]) -> list[CClass]:
        if not self.owner.is_fetched():
            raise RuntimeError(
                f"Owner collection {self.owner.__name__!r} was not fetched yet. Did you call fetch_all()?"
            )
        return self.owner.get_data(self.field_name)


def FeatureFetchSpecifier(  # noqa: N802, a fake class factory
    feature: type[CClass], start_date: DateOrDelta | None = None, end_date: DateOrDelta | None = None
) -> list[CClass]:
    """Create a feature fetch specifier for the given feature model class.

    Args:
        feature: The feature model class to fetch.
        start_date: The start date or delta from now to fetch the data.
        end_date: The end date or delta from now to fetch the data.
    """
    return cast(list[CClass], _FeatureFetchSpecifier(feature, start_date, end_date))
