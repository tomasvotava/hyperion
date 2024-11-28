"""The data catalog."""

import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO, Any, BinaryIO, ClassVar, TypedDict
from uuid import uuid4

import botocore.exceptions
import fastavro
import fastavro.validation
import fastavro.write

from hyperion.catalog.schema import SchemaStore
from hyperion.config import storage_config
from hyperion.entities.catalog import AssetProtocol, DataLakeAsset, FeatureAsset, PersistentStoreAsset
from hyperion.infrastructure.aws import S3Client
from hyperion.infrastructure.queue import ArrivalEvent, DataLakeArrivalMessage, Queue
from hyperion.logging import get_logger

__all__ = ["AssetNotFoundError", "Catalog", "CatalogError"]

logger = get_logger("catalog")


class StoreBucketConfig(TypedDict):
    bucket: str
    prefix: str


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
    # TODO: Unfinished business
    # https://github.com/Zephyr-Trade/FVE-map/issues/9
    _instances: ClassVar[dict[tuple[PersistentStoreAsset, str, str], "PersistentStore"]] = {}

    def __new__(cls, *args: Any, **kwargs: Any) -> "PersistentStore":
        init_arguments = _unpack_args(*args, **kwargs)
        if init_arguments not in cls._instances:
            cls._instances[init_arguments] = super().__new__(cls)
        return cls._instances[init_arguments]

    def __init__(
        self, asset: PersistentStoreAsset, persistent_store_bucket: str, persistent_store_prefix: str = ""
    ) -> None:
        self.asset = asset
        self.persistent_store_bucket = persistent_store_bucket
        self.persistent_store_prefix = persistent_store_prefix
        self._local_path: Path | None = None
        self._etag: str | None = None

    def cleanup(self) -> None:
        if self._local_path is None:
            logger.debug("Persistent store was not retrieved, nothing to clean up.", asset=self.asset)
            return
        logger.info(
            "Cleaning up previously retrieved persistent store.", asset=self.asset, path=self._local_path.as_posix()
        )
        self._local_path.unlink(missing_ok=True)
        self._local_path = None

    def retrieve(self) -> None:
        s3_client = S3Client()
        try:
            remote_etag = s3_client.get_object_attributes(
                self.persistent_store_bucket, self.asset.get_path(self.persistent_store_prefix)
            ).etag
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                raise AssetNotFoundError(self.asset) from error
            raise
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
        s3_client.download(self.persistent_store_bucket, self.asset.get_path(self.persistent_store_prefix), local_path)
        self._local_path = local_path
        self._etag = remote_etag

    def __enter__(self) -> None:
        self.retrieve()

    def __exit__(self, *args: Any) -> None:
        self.cleanup()


class WritablePersistentStore(PersistentStore):
    # TODO: Unfinished business
    # https://github.com/Zephyr-Trade/FVE-map/issues/9
    def store(self, data: Iterable[dict[str, Any]]) -> None:
        with tempfile.TemporaryFile("+wb") as file:
            logger.info("Pouring persistent store asset into temporary file.", asset=self.asset, path=file.name)
            schema = SchemaStore.from_config().get_asset_schema(self.asset)
            _write_avro(file, schema, data, self.asset.to_metadata())
            s3_client = S3Client()
            s3_client.upload(file, self.persistent_store_bucket, self.asset.get_path(self.persistent_store_prefix))


def _write_avro(
    fp: BinaryIO | IO[bytes], schema: dict[str, Any], data: Iterable[dict[str, Any]], metadata: dict[str, str]
) -> None:
    fastavro.writer(
        fp,
        records=data,
        schema=schema,
        codec="deflate",
        validator=True,
        codec_compression_level=7,
        strict=False,
        strict_allow_default=True,
        metadata=metadata,
    )


class Catalog:
    def __init__(
        self,
        *,
        data_lake_bucket: str,
        feature_store_bucket: str,
        persistent_store_bucket: str,
        data_lake_prefix: str = "",
        feature_store_prefix: str = "",
        persistent_store_prefix: str = "",
        queue: Queue | None = None,
    ) -> None:
        self.data_lake_bucket = data_lake_bucket
        self.feature_store_bucket = feature_store_bucket
        self.persistent_store_bucket = persistent_store_bucket
        self.data_lake_prefix = data_lake_prefix
        self.feature_store_prefix = feature_store_prefix
        self.persistent_store_prefix = persistent_store_prefix
        self._config_map: dict[type[AssetProtocol], StoreBucketConfig] = {
            DataLakeAsset: {"bucket": self.data_lake_bucket, "prefix": self.data_lake_prefix},
            PersistentStoreAsset: {"bucket": self.persistent_store_bucket, "prefix": self.persistent_store_prefix},
            FeatureAsset: {"bucket": self.feature_store_bucket, "prefix": self.feature_store_prefix},
        }
        self._s3_client: S3Client | None = None
        self.queue = queue or Queue.from_config()

    @property
    def s3_client(self) -> S3Client:
        if self._s3_client is None:
            self._s3_client = S3Client()
        return self._s3_client

    @staticmethod
    def from_config() -> "Catalog":
        return Catalog(
            data_lake_bucket=storage_config.data_lake_bucket,
            feature_store_bucket=storage_config.feature_store_bucket,
            persistent_store_bucket=storage_config.persistent_store_bucket,
            data_lake_prefix=storage_config.data_lake_prefix,
            feature_store_prefix=storage_config.data_lake_prefix,
            persistent_store_prefix=storage_config.persistent_store_prefix,
        )

    def _store_asset(self, asset: AssetProtocol, data: Iterable[dict[str, Any]]) -> None:
        """Store an asset in its bucket."""
        store_config = self._get_store_config(asset)
        logger.info("Preparing asset storage.", asset=asset, **store_config)
        with tempfile.NamedTemporaryFile("+wb") as file:
            schema = SchemaStore.from_config().get_asset_schema(asset)
            path = Path(file.name)
            logger.info("Pouring asset into a temporary file.", asset=asset, file=path.as_posix())
            _write_avro(file, schema, data, asset.to_metadata())
            logger.info("Avro file was created successfully.", path=path.as_posix(), size=file.tell())
            file.seek(0)

            self.s3_client.upload(file, bucket=store_config["bucket"], name=asset.get_path(store_config["prefix"]))

    def store_data_lake_asset(self, asset: DataLakeAsset, data: Iterable[dict[str, Any]], notify: bool = True) -> None:
        """Store asset specified by its metadata and data generator into a fitting place in the data lake."""
        self._store_asset(asset, data)
        if not notify:
            return
        message = DataLakeArrivalMessage(asset=asset, event=ArrivalEvent.ARRIVED)
        logger.info("Sending data lake arrival message.", asset=asset, message=message, queue=self.queue)
        self.queue.send(message)

    def store_persistent_data(self, asset: PersistentStoreAsset, data: Iterable[dict[str, Any]]) -> None:
        """Store data in a persistent store."""
        self._store_asset(asset, data)

    def store_feature(self, asset: FeatureAsset, data: Iterable[dict[str, Any]]) -> None:
        """Store feature in the feature store."""
        self._store_asset(asset, data)

    def _get_store_config(self, asset: AssetProtocol) -> StoreBucketConfig:
        try:
            return self._config_map[asset.__class__]
        except KeyError:
            logger.error("Attempting to get store config for an unsupported asset tyoe.", asset=asset)
            raise

    def get_asset_file_size(self, asset: AssetProtocol) -> int:
        """Find asset avro file and get its file size in bytes."""
        store_config = self._get_store_config(asset)

        object_path = asset.get_path(store_config["prefix"])
        logger.info("Getting attributes for an asset.", asset=asset, **store_config)
        try:
            return self.s3_client.get_object_attributes(store_config["bucket"], object_path).object_size
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                raise AssetNotFoundError(asset) from error
            raise

    def _retrieve_asset(self, asset: AssetProtocol) -> Iterator[dict[str, Any]]:
        """Retrieve an asset based on its type and store config."""
        store_config = self._get_store_config(asset)
        file_size = self.get_asset_file_size(asset)
        logger.info("Preparing asset for retrieval.", asset=asset, file_size=file_size, **store_config)
        with tempfile.NamedTemporaryFile("+wb") as file:
            logger.info("Downloading asset into a temporary file.", asset=asset, path=file.name)
            self.s3_client.download(store_config["bucket"], asset.get_path(store_config["prefix"]), file)
            file.seek(0)
            for row_number, row in enumerate(fastavro.reader(file), start=1):
                if isinstance(row, dict):
                    yield row
                else:
                    logger.error(
                        "Unexpected data found in data lake asset row.",
                        asset=asset,
                        expected="dict",
                        row_number=row_number,
                        got=str(type(row)),
                    )
                    raise TypeError("Unexpected data received when reading asset data.")

    def retrieve_data_lake_asset(self, asset: DataLakeAsset) -> Iterator[dict[str, Any]]:
        """Retrieve a data lake asset specified by its metadata and iterate its content."""
        return self._retrieve_asset(asset)

    def retrieve_persistent_data(self, asset: PersistentStoreAsset) -> Iterator[dict[str, Any]]:
        """Retrieve a persistent store asset specified by its metadata and iterate its content."""
        return self._retrieve_asset(asset)

    def retrieve_feature(self, asset: FeatureAsset) -> Iterator[dict[str, Any]]:
        """Retrieve a feature store data."""
        return self._retrieve_asset(asset)
