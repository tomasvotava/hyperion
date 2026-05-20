"""The data catalog."""

import asyncio
import datetime
import io
import re
import shutil
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, BinaryIO, ClassVar, Generic, TypeAlias, TypeVar, cast
from uuid import uuid4

from hyperion.adapters.serialization.avro import AvroSerializer, AvroStreamWriter
from hyperion.asyncutils import AsyncTaskQueue, aiter_any
from hyperion.dateutils import (
    TimeResolution,
    TimeResolutionUnit,
    assure_timezone,
    iter_dates_between,
    quantize_datetime,
    truncate_datetime,
    utcnow,
)
from hyperion.domain.assets import (
    AssetProtocol,
    AssetType,
    DataLakeAsset,
    FeatureAsset,
    PersistentStoreAsset,
)
from hyperion.domain.messages import ArrivalEvent, DataLakeArrivalMessage
from hyperion.log import get_logger
from hyperion.ports.queue import Queue
from hyperion.ports.schema_registry import SchemaStore
from hyperion.ports.storage import ObjectNotFoundError, StoragePort

if TYPE_CHECKING:
    from hyperion.ports.cache import Cache

__all__ = ["AssetNotFoundError", "Catalog", "CatalogError"]

logger = get_logger("catalog")

RepartitionableAssetType: TypeAlias = FeatureAsset | DataLakeAsset
RepartitionableAsset = TypeVar("RepartitionableAsset", bound=RepartitionableAssetType)


class CatalogError(Exception):
    """Base class for catalog errors."""


class AssetNotFoundError(CatalogError):
    """Raised when an asset is not found in the catalog."""

    def __init__(self, asset: "AssetProtocol") -> None:
        super().__init__(f"Asset {asset.name!r} not found in the catalog.")


def _unpack_args(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
    arguments = (*args,)
    for _, value in sorted(kwargs.items(), key=lambda pair: pair[0]):
        arguments += (value,)
    return arguments


class PersistentStore:
    """A persistent store for assets."""

    # TODO: Unfinished business
    # https://github.com/Zephyr-Trade/FVE-map/issues/9
    _instances: ClassVar[dict[tuple[Any, ...], "PersistentStore"]] = {}

    def __new__(cls, *args: Any, **kwargs: Any) -> "PersistentStore":
        init_arguments = _unpack_args(*args, **kwargs)
        if init_arguments not in cls._instances:
            cls._instances[init_arguments] = super().__new__(cls)
        return cls._instances[init_arguments]

    def __init__(self, asset: PersistentStoreAsset, storage: StoragePort) -> None:
        """Initialize the persistent store.

        Args:
            asset (PersistentStoreAsset): The asset to store.
            storage (StoragePort): The storage backend the asset lives in.
        """
        self.asset = asset
        self.storage = storage
        self._local_path: Path | None = None
        self._etag: str | None = None

    def cleanup(self) -> None:
        """Clean up the persistent store.

        This method deletes the local file if it exists.
        """
        if self._local_path is None:
            logger.debug("Persistent store was not retrieved, nothing to clean up.", asset=self.asset)
            return
        logger.info(
            "Cleaning up previously retrieved persistent store.", asset=self.asset, path=self._local_path.as_posix()
        )
        self._local_path.unlink(missing_ok=True)
        self._local_path = None

    def retrieve(self) -> None:
        """Retrieve the persistent store from its storage backend."""
        try:
            remote_etag = self.storage.get_attributes(self.asset.get_path()).etag
        except ObjectNotFoundError as error:
            raise AssetNotFoundError(self.asset) from error
        if self._local_path is not None:
            if remote_etag == self._etag:
                logger.info(
                    "Persistent store previously retrieved.",
                    asset=self.asset,
                    path=self._local_path.as_posix(),
                    etag=remote_etag,
                )
                return
            logger.info(
                "Forcing re-download of a an outdated previously retrieved store.",
                asset=self.asset,
                local_etag=self._etag,
                remote_etag=remote_etag,
            )
            self.cleanup()

        local_path = Path(tempfile.gettempdir()) / f"{uuid4().hex}.asset"
        logger.info("Retrieving persistent store.", asset=self.asset, path=local_path.as_posix())
        with self.storage.open(self.asset.get_path()) as source, local_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        self._local_path = local_path
        self._etag = remote_etag

    def __enter__(self) -> None:
        self.retrieve()

    def __exit__(self, *args: Any) -> None:
        self.cleanup()


class WritablePersistentStore(PersistentStore):
    """A writable persistent store for assets."""

    # TODO: Unfinished business
    # https://github.com/Zephyr-Trade/FVE-map/issues/9
    def store(self, data: Iterable[dict[str, Any]]) -> None:
        """Store data in the persistent store.

        Args:
            data (Iterable[dict[str, Any]]): The data to store.
        """
        with tempfile.TemporaryFile("+wb") as file:
            logger.info("Pouring persistent store asset into temporary file.", asset=self.asset, path=file.name)
            schema = SchemaStore.from_config().get_asset_schema(self.asset)
            AvroSerializer().write(file, schema, data, self.asset.to_metadata())
            file.seek(0)
            self.storage.put(self.asset.get_path(), file)


class Catalog:
    """The data catalog.

    The catalog is responsible for storing and retrieving assets.
    """

    _ASSET_TYPES: ClassVar[tuple[AssetType, ...]] = ("data_lake", "feature", "persistent_store")
    DEFAULT_SPOOL_THRESHOLD_BYTES: ClassVar[int] = 8 * 1024 * 1024

    def __init__(
        self,
        *,
        storage: StoragePort | Mapping[AssetType, StoragePort],
        queue: Queue | None = None,
        cache: "Cache | None" = None,
        schema_store: "SchemaStore | None" = None,
        serializer: AvroSerializer | None = None,
        spool_threshold_bytes: int = DEFAULT_SPOOL_THRESHOLD_BYTES,
    ) -> None:
        """Initialize the catalog.

        Args:
            storage (StoragePort | Mapping[AssetType, StoragePort]): The storage
                backend. A single port serves every asset type; pass a mapping
                keyed by asset type ("data_lake"/"feature"/"persistent_store")
                to route each type to a different backend (this is how
                :meth:`from_config` preserves today's per-bucket S3 layout).
            queue (Queue, optional): The queue to use for notifications. Defaults to None.
            cache (Cache, optional): The cache to use for storing assets for quicker re-retrieval. Defaults to None.
            schema_store (SchemaStore, optional): The schema store. Defaults to ``SchemaStore.from_config()``.
            serializer (AvroSerializer, optional): The avro serializer. Defaults to a fresh ``AvroSerializer``.
            spool_threshold_bytes (int, optional): Upper bound (inclusive) for in-memory
                avro serialization on the store-asset path. Payloads at or below this size
                upload from a BytesIO buffer; larger payloads are promoted to an on-disk
                temp file. Defaults to 8 MiB.
        """
        if isinstance(storage, Mapping):
            missing = [asset_type for asset_type in self._ASSET_TYPES if asset_type not in storage]
            if missing:
                raise ValueError(f"storage mapping is missing adapters for asset types: {missing}.")
            self._storage: dict[AssetType, StoragePort] = dict(storage)
        else:
            self._storage = {asset_type: storage for asset_type in self._ASSET_TYPES}
        self._serializer = serializer or AvroSerializer()
        self.queue = queue or Queue.from_config()
        self.cache = cache
        if self.cache is not None and not self.cache.hash_keys:
            logger.warning(
                "It is recommended to hash keys when caching catalog assets, "
                "because asset paths may contain unsafe characters."
            )

        self.schema_store = schema_store or SchemaStore.from_config()
        self.spool_threshold_bytes = spool_threshold_bytes

    def _resolve_storage(self, asset: AssetProtocol | type[AssetProtocol]) -> StoragePort:
        try:
            return self._storage[asset.asset_type]
        except KeyError:
            logger.error("No storage configured for asset type.", asset=asset, asset_type=asset.asset_type)
            raise

    @staticmethod
    def from_config() -> "Catalog":
        """Create a catalog from the configuration.

        Builds one :class:`~hyperion.adapters.storage.s3.S3Storage` per asset
        type so the on-S3 key layout (bucket + prefix per store) is identical to
        the pre-refactor catalog. (Fixes a long-standing bug where the feature
        store used the *data lake* prefix.)
        """
        from hyperion import composition

        return Catalog(storage=composition.default_storage())

    def _serialize_asset_to_tempfile(
        self, asset: AssetProtocol, data: Iterable[dict[str, Any]], schema_path: str | None = None
    ) -> io.BytesIO | Path:
        """Serialize ``data`` for ``asset`` and return either an in-memory buffer or a temp-file path.

        Payloads up to ``spool_threshold_bytes`` are returned as an open
        :class:`io.BytesIO` seeked to 0, so the caller can upload from RAM
        without a disk round-trip. Larger payloads are spilled to a
        ``NamedTemporaryFile(delete=False)`` and the :class:`~pathlib.Path` is
        returned; the caller owns that path and must unlink it.

        fastavro only requires ``seek`` / ``tell`` on the destination, which
        BytesIO provides natively.
        """
        schema = (
            self.schema_store.get_asset_schema(asset)
            if schema_path is None
            else self.schema_store.get_schema_from_path(schema_path)
        )
        buffer = io.BytesIO()
        logger.info("Pouring asset into an in-memory buffer.", asset=asset)
        self._serializer.write(buffer, schema, data, asset.to_metadata())
        size = buffer.tell()
        if size <= self.spool_threshold_bytes:
            buffer.seek(0)
            logger.info("Avro payload kept in memory.", asset=asset, size=size)
            return buffer
        file = tempfile.NamedTemporaryFile("+wb", delete=False)  # noqa: SIM115
        path = Path(file.name)
        try:
            logger.info(
                "Avro payload exceeded spool threshold; spilling to disk.",
                asset=asset,
                size=size,
                threshold=self.spool_threshold_bytes,
                file=path.as_posix(),
            )
            buffer.seek(0)
            shutil.copyfileobj(buffer, file)
        except BaseException:
            file.close()
            path.unlink(missing_ok=True)
            raise
        finally:
            buffer.close()
        file.close()
        return path

    @contextmanager
    def _prepare_asset_storage(
        self, asset: AssetProtocol, data: Iterable[dict[str, Any]], schema_path: str | None = None
    ) -> Iterator[IO[bytes]]:
        prepared = self._serialize_asset_to_tempfile(asset, data, schema_path)
        if isinstance(prepared, Path):
            try:
                with prepared.open("rb") as file:
                    yield file
            finally:
                prepared.unlink(missing_ok=True)
            return
        try:
            yield prepared
        finally:
            prepared.close()

    async def store_asset_async(
        self, asset: AssetProtocol, data: Iterable[dict[str, Any]], notify: bool = True, schema_path: str | None = None
    ) -> None:
        """Store an asset in its bucket asynchronously.

        Args:
            asset (AssetProtocol): The asset to store.
            data (Iterable[dict[str, Any]]): The data to store.
            notify (bool, optional): Whether to send a notification. Defaults to True.
        """
        logger.info("Preparing asset storage.", asset=asset)
        prepared = await asyncio.to_thread(self._serialize_asset_to_tempfile, asset, data, schema_path)
        if isinstance(prepared, Path):
            try:
                with prepared.open("rb") as file:
                    await self._resolve_storage(asset).put_async(asset.get_path(), file)
            finally:
                prepared.unlink(missing_ok=True)
        else:
            try:
                await self._resolve_storage(asset).put_async(asset.get_path(), prepared)
            finally:
                prepared.close()
        if notify:
            self._notify_asset_arrival(asset, schema_path=schema_path)

    def _notify_asset_arrival(self, asset: AssetProtocol, schema_path: str | None = None) -> None:
        if not isinstance(asset, DataLakeAsset):
            logger.debug("Skipping notification for asset, not DataLakeAsset type.", asset=asset)
            return
        message = DataLakeArrivalMessage(asset=asset, event=ArrivalEvent.ARRIVED, schema_path=schema_path)
        logger.info("Sending data lake arrival message.", asset=asset, message=message, queue=self.queue)
        self.queue.send(message)

    def get_feature_data(
        self,
        name: str,
        resolution: TimeResolution | str,
        the_now: datetime.datetime | None = None,
        tolerance: int = 0,
    ) -> Iterator[dict[str, Any]]:
        resolution = resolution if isinstance(resolution, TimeResolution) else TimeResolution.from_str(resolution)
        the_now = the_now or utcnow()
        try_timestamps = [the_now - resolution.delta * i for i in range(0, tolerance + 1)]
        for timestamp in try_timestamps:
            feature_partition_date = quantize_datetime(timestamp, resolution)
            feature_asset = FeatureAsset(name, feature_partition_date, resolution)
            logger.debug(
                f"Trying to find feature data for feature {feature_asset.feature_name!r}.", feature_asset=feature_asset
            )
            try:
                self.get_asset_file_size(feature_asset)
            except AssetNotFoundError as error:
                logger.error(f"Feature asset was not found - {error}.", feature_asset=feature_asset)
                continue
            return self.retrieve_asset(feature_asset)
        raise AssetNotFoundError(feature_asset)

    def store_asset(
        self, asset: AssetProtocol, data: Iterable[dict[str, Any]], notify: bool = True, schema_path: str | None = None
    ) -> None:
        """Store an asset in its bucket.

        Args:
            asset (AssetProtocol): The asset to store.
            data (Iterable[dict[str, Any]]): The data to store.
            notify (bool, optional): Whether to send a notification. Defaults to True.
        """
        logger.info("Preparing asset storage.", asset=asset)
        with self._prepare_asset_storage(asset, data, schema_path) as file:
            self._resolve_storage(asset).put(asset.get_path(), file)
        if notify:
            self._notify_asset_arrival(asset, schema_path=schema_path)

    def get_asset_file_size(self, asset: AssetProtocol) -> int:
        """Find asset avro file and get its file size in bytes.

        Args:
            asset (AssetProtocol): The asset to get the file size for.

        Returns:
            int: The file size in bytes.
        """
        logger.info("Getting attributes for an asset.", asset=asset)
        try:
            return self._resolve_storage(asset).get_attributes(asset.get_path()).size
        except ObjectNotFoundError as error:
            raise AssetNotFoundError(asset) from error

    def _get_cache_key(self, asset: AssetProtocol) -> str:
        return f"{asset.asset_type}:{asset.get_path()}"

    def _iter_data_from_downloaded_asset(
        self, file: BinaryIO | IO[bytes], asset: AssetProtocol
    ) -> Iterator[dict[str, Any]]:
        for row_number, row in enumerate(self._serializer.read(file), start=1):
            if isinstance(row, dict):
                yield row
            else:
                logger.error(
                    "Unexpected data found in a downloaded asset row.",
                    asset=asset,
                    expected="dict",
                    row_number=row_number,
                    got=str(type(row)),
                )
                raise TypeError(f"Unexpected data received when reading downloaded asset data, row {row_number}")

    def _download_asset_into_file(self, asset: AssetProtocol, file: IO[bytes]) -> None:
        logger.info("Downloading asset into a file.", asset=asset, path=getattr(file, "name", None) or "unnamed")
        try:
            with self._resolve_storage(asset).open(asset.get_path()) as source:
                shutil.copyfileobj(source, file)
        except ObjectNotFoundError as error:
            raise AssetNotFoundError(asset) from error

    @contextmanager
    def _get_asset_file_handle(self, asset: AssetProtocol, *, no_cache: bool = False) -> Iterator[IO[bytes]]:
        if no_cache or self.cache is None:
            with tempfile.NamedTemporaryFile("+wb") as file:
                self._download_asset_into_file(asset, file)
                file.seek(0)
                yield file
            return
        cache_key = self._get_cache_key(asset)
        hit = self.cache.hit(cache_key)
        with self.cache.open(cache_key, "bytes") as cache_file:
            if not hit:
                self._download_asset_into_file(asset, cache_file)
            cache_file.seek(0)
            yield cache_file

    def retrieve_asset(self, asset: AssetProtocol) -> Iterator[dict[str, Any]]:
        """Retrieve an asset based on its type and store config.

        Args:
            asset (AssetProtocol): The asset to retrieve.

        Yields:
            dict[str, Any]: The asset data.
        """
        with self._get_asset_file_handle(asset) as file:
            yield from self._iter_data_from_downloaded_asset(file, asset)

    def iter_datalake_partitions(self, asset_name: str, date_part: str | None = None) -> Iterator[DataLakeAsset]:
        """Iterate over data lake partitions.

        Providing the date part can significantly reduce the number of keys to iterate over.
        Partition dates are stored as ISO formatted strings, therefore the date part should be in the same format.
        E.g. to only iterate over partitions for January 2025, provide '2025-01'.

        Args:
            asset_name (str): The name of the asset.
            date_part (str, optional): The date part to filter by. Defaults to None.

        Yields:
            DataLakeAsset: The data lake asset.
        """
        storage = self._resolve_storage(DataLakeAsset)
        # The adapter owns its own storage-side prefix and yields keys relative
        # to it, so iterate in the bare asset namespace.
        keys_prefix = f"{asset_name}/date={date_part if date_part else ''}"
        version_patt = re.compile(r"v(?P<version>\d+)\.avro")
        for key in storage.iter_keys(keys_prefix):
            try:
                key_asset_name, partition, filename = key.split("/")
            except ValueError:
                logger.warning(
                    "The key path does not have 'Asset/Partition/Version' format and will be skipped.",
                    key=key,
                )
                continue
            if key_asset_name != asset_name:
                logger.warning(
                    "The key path does not match the asset name and will be skipped.", key=key, asset_name=asset_name
                )
                continue
            if (match := version_patt.match(filename)) is None:
                logger.warning("The key path does not match the version pattern and will be skipped.", key=key)
                continue
            version = int(match.group("version"))
            partition_date_str = partition.split("date=")[1]
            partition_date = datetime.datetime.fromisoformat(partition_date_str)
            logger.debug(
                "Found data lake partition.", asset_name=asset_name, partition_date=partition_date, version=version
            )
            yield DataLakeAsset(asset_name, assure_timezone(partition_date), version)

    def find_latest_datalake_partition(self, asset_name: str, date_part: str | None = None) -> DataLakeAsset:
        """Find the latest data lake partition.

        Providing the date part can significantly reduce the number of keys to iterate over.
        Partition dates are stored as ISO formatted strings, therefore the date part should be in the same format.
        E.g. to only iterate over partitions for January 2025, provide '2025-01'.

        Args:
            asset_name (str): The name of the asset.
            date_part (str, optional): The date part to filter by. Defaults to None.

        Returns:
            DataLakeAsset: The latest data lake partition.
        """
        return next(
            iter(
                sorted(
                    self.iter_datalake_partitions(asset_name, date_part),
                    key=lambda partition: partition.date,
                    reverse=True,
                )
            )
        )

    def iter_feature_store_partitions(
        self,
        feature_name: str,
        resolution: TimeResolution | str,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        version: int = 1,
    ) -> Iterator[FeatureAsset]:
        """Iterate over feature store partitions relevant for a given time range.

        For a given time range, finds all feature store partitions that could contain
        data for that range based on the feature's resolution. For example, for dates
        between 2025-01-01 and 2025-01-15 with 7d resolution, this would check partitions
        2025-01-08, 2025-01-15, and 2025-01-22, since data points from those dates would
        be stored in these quantized partitions.

        Args:
            feature_name (str): The name of the feature.
            resolution (TimeResolution | str): The time resolution of the feature.
            date_from (datetime.datetime): Start of the time range to find partitions for.
            date_to (datetime.datetime): End of the time range to find partitions for.
            version (int): Schema version of the feature. Defaults to 1.

        Yields:
            FeatureAsset: Feature store assets that could contain data for the time range.
        """
        resolution = resolution if isinstance(resolution, TimeResolution) else TimeResolution.from_str(resolution)

        dates = iter_dates_between(date_from, date_to, resolution.unit)

        partition_dates = {quantize_datetime(date, resolution) for date in dates}

        for partition_date in sorted(partition_dates):
            feature_asset = FeatureAsset(feature_name, partition_date, resolution, schema_version=version)

            try:
                self.get_asset_file_size(feature_asset)
                logger.debug("Found partition for the feature.", asset=feature_asset)
                yield feature_asset

            except AssetNotFoundError:
                logger.debug("No feature store partition found for timestamp.", asset=feature_asset)
                continue

    async def repartition(
        self,
        asset: DataLakeAsset | FeatureAsset,
        granularity: TimeResolutionUnit,
        date_attribute: str = "timestamp",
        data: Iterable[dict[str, Any]] | None = None,
    ) -> None:
        """Repartition a data lake asset based on a time resolution unit.

        If data is not provided, the asset is retrieved from the catalog.

        Args:
            asset (DataLakeAsset): The asset to repartition.
            granularity (TimeResolutionUnit): The time resolution unit to use.
            date_attribute (str, optional): The date attribute to use. Defaults to "timestamp".
            data (Iterable[dict[str, Any]], optional): The data to repartition. Defaults to None.
        """
        repartitioner = AssetRepartitioner(self, asset, granularity, date_attribute)
        await repartitioner.repartition(data)


class AssetRepartitioner(Generic[RepartitionableAsset]):
    """A class to repartition a data lake asset based on a time resolution unit."""

    def __init__(
        self,
        catalog: Catalog,
        asset: RepartitionableAsset,
        granularity: TimeResolutionUnit,
        date_attribute: str = "timestamp",
    ) -> None:
        """Initialize the repartitioner.

        Args:
            catalog (Catalog): The catalog to use.
            asset (DataLakeAsset | FeatureAsset): The asset to repartition.
            granularity (TimeResolutionUnit): The time resolution unit to use.
            date_attribute (str, optional): The date attribute to use. Defaults to "timestamp".
        """
        self.catalog = catalog
        self.asset = asset
        self.granularity = granularity
        self.date_attribute = date_attribute
        self._partition_name = "date" if isinstance(self.asset, DataLakeAsset) else "timestamp"

        self._state: dict[datetime.datetime, tuple[IO[bytes], RepartitionableAsset, AvroStreamWriter]] = {}

    def __enter__(self) -> None:
        self._state = {}

    def __exit__(self, *args: Any) -> None:
        for file, _, __ in self._state.values():
            logger.info("Closing temporary file.", path=file.name)
            file.close()

    def _create_partition_asset(self, partition_date: datetime.datetime) -> RepartitionableAsset:
        if isinstance(self.asset, DataLakeAsset):
            return cast(RepartitionableAsset, DataLakeAsset(self.asset.name, partition_date, self.asset.schema_version))
        if isinstance(self.asset, FeatureAsset):
            return cast(
                RepartitionableAsset,
                FeatureAsset(
                    self.asset.name,
                    partition_date,
                    self.asset.resolution,
                    self.asset.schema_version,
                    self.asset.partition_keys,
                ),
            )
        raise TypeError(f"Unsupported asset type {type(self.asset)!r}.")

    def _get_handler(
        self, partition_date: datetime.datetime
    ) -> tuple[IO[bytes], RepartitionableAsset, AvroStreamWriter]:
        if partition_date in self._state:
            return self._state[partition_date]
        logger.info("Creating a new handler for partition date.", partition_date=partition_date.isoformat())
        file = tempfile.NamedTemporaryFile("+wb")  # noqa: SIM115
        logger.info(f"Partition will be temporarily stored in {file.name!r}.", path=file.name)
        asset = self._create_partition_asset(partition_date)
        writer = self.catalog._serializer.streaming_writer(
            file, self.catalog.schema_store.get_asset_schema(asset), asset.to_metadata()
        )
        handler = (file, asset, writer)
        self._state[partition_date] = handler
        return handler

    def _write_records(self, data: Iterable[dict[str, Any]]) -> None:
        """Drain ``data`` into per-partition streaming writers (blocking)."""
        for record in data:
            timestamp = record.get(self.date_attribute)
            if not isinstance(timestamp, datetime.datetime):
                raise ValueError(
                    f"Asset {self.asset!r} cannot be repartitioned using date attribute "
                    f"{self.date_attribute!r} - it is not a valid datetime."
                )
            partition_date = truncate_datetime(timestamp, self.granularity)
            _, __, writer = self._get_handler(partition_date)
            writer.write(record)

    async def repartition(self, data: Iterable[dict[str, Any]] | None = None) -> None:
        """Repartition the asset.

        This method reads the asset data, partitions it based on the date attribute and granularity,
        and uploads the partitioned data to the data lake bucket.
        If data is not provided, the asset is retrieved from the catalog.

        Args:
            data (Iterable[dict[str, Any]], optional): The data to repartition. Defaults to None.
        """
        data = data or self.catalog.retrieve_asset(self.asset)
        with self:
            await asyncio.to_thread(self._write_records, data)
            logger.info("Finished creating partitioned avro files.")

            async with AsyncTaskQueue[None](maxsize=5) as queue:
                async for file, asset, writer in aiter_any(self._state.values()):
                    logger.info("Dumping and uploading asset from temporary file.", asset=asset, path=file.name)
                    await asyncio.to_thread(writer.dump)
                    file.seek(0)
                    await queue.add_task(self.catalog._resolve_storage(asset).put_async(asset.get_path(), file))
