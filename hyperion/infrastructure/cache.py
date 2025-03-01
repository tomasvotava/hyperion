"""A serverless cache for our shenanigans."""

import hashlib
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, cast

import boto3
import cachetools
import snappy

from hyperion.catalog import AssetNotFoundError, Catalog
from hyperion.config import storage_config
from hyperion.dateutils import utcnow
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.logging import get_logger

DEFAULT_TTL_SECONDS = 60
DYNAMODB_MAX_LENGTH = 65535
DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE = 256 * (1024**2)

logger = get_logger("cache")


class CachingError(Exception):
    pass


class Cache(ABC):
    """A serverless cache for our shenanigans."""

    _instances: ClassVar[dict[tuple[str, bool], "Cache"]] = {}

    @classmethod
    def from_config(cls) -> "Cache":
        """
        Creates a cache from the configuration.

        This function emulates a singleton pattern, so it will return the same instance for the same configuration.

        Returns:
            Cache: A cache instance.
        """
        instance_key = (storage_config.cache_key_prefix, True)
        if instance_key not in Cache._instances:
            if storage_config.cache_dynamodb_table:
                logger.info("Using DynamoDB Cache.")
                cls._instances[instance_key] = DynamoDBCache(
                    prefix=storage_config.cache_key_prefix,
                    default_ttl=storage_config.cache_dynamodb_default_ttl,
                    table_name=storage_config.cache_dynamodb_table,
                )
            elif storage_config.cache_local_path:
                logger.info("Using LocalFileCache.", path=storage_config.cache_local_path)
                cls._instances[instance_key] = LocalFileCache(
                    prefix=storage_config.cache_key_prefix, root_path=Path(storage_config.cache_local_path)
                )
            else:
                logger.info("Using InMemory Cache.")
                cls._instances[instance_key] = InMemoryCache(
                    prefix=storage_config.cache_key_prefix, default_ttl=storage_config.cache_dynamodb_default_ttl
                )
        return cls._instances[instance_key]

    def __init__(self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS):
        """Initializes the cache with the given prefix and default TTL.

        Args:
            prefix (str): The prefix for the cache keys.
            hash_keys (bool): Whether to hash the keys.
            default_ttl (int): The default TTL for the cache.
        """
        self.prefix = prefix
        self.hash_keys = hash_keys
        self.default_ttl = default_ttl

    def _key(self, key: str) -> str:
        """Generates a cache key from a given key."""
        if self.hash_keys:
            key = hashlib.sha256(key.encode(encoding="utf-8")).hexdigest()
        prefix = f"{self.prefix}:" if self.prefix else ""
        return f"{prefix}{key}"

    def _compress(self, value: str) -> bytes:
        """Compresses a value using snappy compression."""
        compressed_value = snappy.compress(value.encode(encoding="utf-8"))
        logger.debug(
            "Compressed value using snappy compression.",
            original_length=len(value),
            compressed_length=len(compressed_value),
            ratio=len(compressed_value) / len(value),
        )
        return cast(bytes, compressed_value)

    def _decompress(self, value: bytes) -> str:
        """Decompresses a value using snappy decompression."""
        return cast(str, snappy.decompress(value).decode(encoding="utf-8"))

    @abstractmethod
    def get(self, key: str) -> str | None:
        """Gets a value from the cache."""
        pass

    @abstractmethod
    def set(self, key: str, value: str) -> None:
        """Sets a value in the cache."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Deletes a value from the cache."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clears the cache."""
        pass

    @abstractmethod
    def hit(self, key: str) -> bool:
        """Checks if a key exists in the cache."""
        pass


class InMemoryCache(Cache):
    """An in-memory cache for our shenanigans."""

    MAX_KEYS = 1000

    def __init__(
        self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS, max_size: int = MAX_KEYS
    ):
        super().__init__(prefix, hash_keys, default_ttl)
        self.max_size = max_size
        self.cache = cachetools.TTLCache[str, str](maxsize=self.max_size, ttl=self.default_ttl)

    def get(self, key: str) -> str | None:
        return self.cache.get(self._key(key))

    def set(self, key: str, value: str) -> None:
        self.cache[self._key(key)] = value

    def delete(self, key: str) -> None:
        self.cache.pop(self._key(key), None)

    def clear(self) -> None:
        self.cache.clear()

    def hit(self, key: str) -> bool:
        return self._key(key) in self.cache


class LocalFileCache(Cache):
    """A local file cache for our shenanigans."""

    def __init__(
        self,
        prefix: str,
        hash_keys: bool = True,
        default_ttl: int = DEFAULT_TTL_SECONDS,
        root_path: Path | None = None,
        max_size: int | None = DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE,
    ) -> None:
        super().__init__(prefix, hash_keys, default_ttl)
        self.root_path = root_path or Path(tempfile.mkdtemp())
        if self.root_path.exists() and not self.root_path.is_dir():
            raise ValueError(f"Given local cache path ({self.root_path.as_posix()}) is not a directory.")
        self.max_size = max_size
        self._assert_root_path()
        logger.info(f"Initialized LocalFileCache in {self.root_path.as_posix()}.", root_path=self.root_path.as_posix())
        if not hash_keys:
            logger.warning("When using filesystem cache, it is recommended to hash keys.")
        self.shrink_to_fit_max_size()

    def _assert_root_path(self) -> None:
        self.root_path.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        """Clean up all expired files from the cache."""
        self._assert_root_path()
        for key_path in self.root_path.iterdir():
            if key_path.is_file() and self._is_expired(key_path):
                logger.debug("Cleaning up expired file.", key_path=key_path)
                key_path.unlink()

    def get_total_size(self) -> int:
        self._assert_root_path()
        return sum(key_path.stat().st_size for key_path in self.root_path.iterdir() if key_path.is_file())

    def shrink_to_fit_max_size(self) -> None:
        self.cleanup()
        if not self.max_size or self.max_size < 0:
            return
        total_size = self.get_total_size()
        keys_ordered = sorted(self.root_path.iterdir(), key=lambda key: key.stat().st_mtime)
        while total_size > self.max_size:
            key_path = keys_ordered.pop(0)
            if not key_path.is_file():
                continue
            size = key_path.stat().st_size
            logger.debug("Cleaning up old file to make some space.", key_path=key_path, size=size)
            key_path.unlink()
            total_size -= size

    def _key_path(self, key: str) -> Path:
        return self.root_path / self._key(key)

    def _is_expired(self, key: str | Path) -> bool:
        self._assert_root_path()
        if isinstance(key, str):
            key = self._key_path(key)
        current_time = time.time()
        return (current_time - key.stat().st_mtime) > self.default_ttl

    def get(self, key: str) -> str | None:
        self._assert_root_path()
        key_path = self._key_path(key)
        if not key_path.exists():
            return None
        if self._is_expired(key_path):
            logger.debug("Key is expired, deleting file.", key=key, key_path=key_path)
            key_path.unlink()
            return None
        logger.debug("Reading key from file.", key=key, path=key_path.as_posix())
        return self._decompress(key_path.read_bytes())

    def set(self, key: str, value: str) -> None:
        self._assert_root_path()
        key_path = self._key_path(key)
        logger.debug("Storing key into a file.", key=key, path=key_path.as_posix())
        key_path.write_bytes(self._compress(value))

    def delete(self, key: str) -> None:
        self._assert_root_path()
        key_path = self._key_path(key)
        if not key_path.exists():
            return None
        logger.debug("Removing cached key.", key=key, path=key_path.as_posix())
        key_path.unlink()

    def hit(self, key: str) -> bool:
        key_path = self._key_path(key)
        return key_path.exists()

    def clear(self) -> None:
        self._assert_root_path()
        for file in self.root_path.iterdir():
            if not file.is_file():
                continue
            logger.debug("Removing cached key.", path=file.as_posix())
            file.unlink()


class PersistentCache(Cache):
    """Uses a persistent store asset to store the cached data.

    Persistent store asset is a key-value store that is stored in the catalog.
    Please note that for now, there is no locking mechanism in place and two services using the same cache
    may overwrite each other's data.

    Note that `default_ttl` and all ttl-related arguments are ignored.
    """

    # TODO: Implement a locking or ownership mechanism
    # https://github.com/Zephyr-Trade/FVE-map/issues/11
    def __init__(
        self,
        prefix: str,
        hash_keys: bool = True,
        default_ttl: int = DEFAULT_TTL_SECONDS,
        asset: "PersistentStoreAsset | None" = None,
        catalog: Catalog | None = None,
    ):
        """Initializes the persistent cache with the given prefix, asset, and catalog.

        Args:
            prefix (str): The prefix for the cache keys.
            hash_keys (bool): Whether to hash the keys.
            default_ttl (int): The default TTL for the cache.
            asset (PersistentStoreAsset, optional): The asset to use for the cache. Must be provided.
            catalog (Catalog, optional): The catalog to use for the cache. Defaults to None
                and creates a new one from the config.
        """
        super().__init__(prefix, hash_keys, default_ttl)
        if asset is None:
            raise ValueError("No asset provided for persistent cache.")
        self.asset = asset
        self.catalog = catalog or Catalog.from_config()
        self._data: dict[str, str] | None = None

    def __enter__(self) -> None:
        """Retrieve the store to work with the cache."""
        try:
            data = self.catalog.retrieve_asset(self.asset)
            self._data = {row["key"]: row["value"] for row in data}
            logger.info(
                f"Retrieved {len(self._data)} items from persistent cache.", asset=self.asset, count=len(self._data)
            )
        except AssetNotFoundError:
            logger.info("Persistent cache not found, creating new cache.", asset=self.asset)
            self._data = {}

    def __exit__(self, *args: Any) -> None:
        """Upload the contents of the cache as the new version of the store."""
        if self._data is None:
            logger.warning("Persistent cache has not yet been retrieved, no data written.", asset=self.asset)
            return None
        timestamp = utcnow()
        data = ({"key": key, "value": value, "timestamp": timestamp} for key, value in self._data.items())
        self.catalog.store_asset(self.asset, data)

    def get(self, key: str) -> str | None:
        cache_key = self._key(key)
        return self.data.get(cache_key)

    @property
    def data(self) -> dict[str, str]:
        if self._data is None:
            raise RuntimeError("Persistent cache must be used as a context manager.")
        return self._data

    def set(self, key: str, value: str) -> None:
        cache_key = self._key(key)
        self.data[cache_key] = value

    def delete(self, key: str) -> None:
        cache_key = self._key(key)
        if cache_key in self.data:
            del self.data[cache_key]

    def clear(self) -> None:
        self.data.clear()

    def hit(self, key: str) -> bool:
        return self._key(key) in self.data


class DynamoDBCache(Cache):
    """A DynamoDB cache for our shenanigans."""

    TTL_ATTRIBUTE_NAME = "time_to_live"

    def __init__(
        self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS, table_name: str | None = None
    ):
        super().__init__(prefix, hash_keys, default_ttl)
        self.client = boto3.resource("dynamodb")
        self.table_name = table_name
        self.table = self.client.Table(table_name)

    def get(self, key: str) -> str | None:
        cache_key = self._key(key)
        response = self.table.get_item(Key={"key": cache_key})
        item = response.get("Item")
        if item:
            return self._decompress(bytes(item["value"]))
        return None

    def set(self, key: str, value: str) -> None:
        expiration_time = int(time.time()) + self.default_ttl
        cache_key = self._key(key)
        compressed_value = self._compress(value)
        if len(compressed_value) > DYNAMODB_MAX_LENGTH:
            logger.warning(
                "Value is too long to store in DynamoDB.",
                original_key=cache_key,
                cache_key=cache_key,
                length=len(compressed_value),
            )
            raise CachingError(f"Value is too long to store in DynamoDB: {len(compressed_value)}")
        self.table.put_item(
            Item={"key": cache_key, "value": compressed_value, self.TTL_ATTRIBUTE_NAME: expiration_time}
        )

    def delete(self, key: str) -> None:
        key = self._key(key)
        self.table.delete_item(Key={"key": key})

    def clear(self) -> None:
        """
        Deletes all items in the table.

        Warning: DynamoDB doesn't have a built-in clear mechanism, so we scan and delete all items manually.
        """
        logger.info("Clearing cache.", cache_table=self.table_name)
        scan = self.table.scan()
        with self.table.batch_writer() as batch:
            for item in scan["Items"]:
                batch.delete_item(Key={"key": item["key"]})

    def hit(self, key: str) -> bool:
        cache_key = self._key(key)
        item = self.table.get_item(Key={"key": cache_key}, ProjectionExpression=self.TTL_ATTRIBUTE_NAME).get("Item")
        return bool(item and int(item.get(self.TTL_ATTRIBUTE_NAME, 0)) > int(time.time()))
