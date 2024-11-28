"""A serverless cache for our shenanigans."""

import datetime
import hashlib
import time
from abc import ABC, abstractmethod
from typing import Any, ClassVar, cast

import boto3
import cachetools
import snappy

from hyperion.catalog import AssetNotFoundError, Catalog
from hyperion.config import storage_config
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.logging import get_logger

DEFAULT_TTL_SECONDS = 60
DYNAMODB_MAX_LENGTH = 65535

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
                cls._instances[instance_key] = DynamoDBCache(
                    prefix=storage_config.cache_key_prefix, default_ttl=storage_config.cache_dynamodb_default_ttl
                )
            else:
                cls._instances[instance_key] = InMemoryCache(
                    prefix=storage_config.cache_key_prefix, default_ttl=storage_config.cache_dynamodb_default_ttl
                )
        return cls._instances[instance_key]

    def __init__(self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS):
        self.prefix = prefix
        self.hash_keys = hash_keys
        self.default_ttl = default_ttl

    def _key(self, key: str) -> str:
        """Generates a cache key from a given key."""
        if not self.hash_keys:
            return key
        key = hashlib.sha256(key.encode(encoding="utf-8")).hexdigest()
        return f"{self.prefix}:{key}"

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
    def set(self, key: str, value: str, ttl: int = DEFAULT_TTL_SECONDS) -> None:
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
    MAX_KEYS = 1000

    def __init__(
        self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS, max_size: int = MAX_KEYS
    ):
        super().__init__(prefix, hash_keys, default_ttl)
        self.max_size = max_size
        self.cache = cachetools.TTLCache[str, str](maxsize=self.max_size, ttl=self.default_ttl)

    def get(self, key: str) -> str | None:
        return self.cache.get(self._key(key))

    def set(self, key: str, value: str, ttl: int = DEFAULT_TTL_SECONDS) -> None:
        self.cache[self._key(key)] = value

    def delete(self, key: str) -> None:
        self.cache.pop(self._key(key), None)

    def clear(self) -> None:
        self.cache.clear()

    def hit(self, key: str) -> bool:
        return self._key(key) in self.cache


class PersistentCache(Cache):
    """Uses a persistent store asset to store the cached data.

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
        super().__init__(prefix, hash_keys, default_ttl)
        if asset is None:
            raise ValueError("No asset provided for persistent cache.")
        self.asset = asset
        self.catalog = catalog or Catalog.from_config()
        self._data: dict[str, str] | None = None

    def __enter__(self) -> None:
        """Retrieve the store to work with the cache."""
        try:
            data = self.catalog.retrieve_persistent_data(self.asset)
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
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        data = ({"key": key, "value": value, "timestamp": timestamp} for key, value in self._data.items())
        self.catalog.store_persistent_data(self.asset, data)

    def get(self, key: str) -> str | None:
        cache_key = self._key(key)
        return self.data.get(cache_key)

    @property
    def data(self) -> dict[str, str]:
        if self._data is None:
            raise RuntimeError("Persistent cache must be used as a context manager.")
        return self._data

    def set(self, key: str, value: str, ttl: int = DEFAULT_TTL_SECONDS) -> None:
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
        if item and int(item.get(self.TTL_ATTRIBUTE_NAME, 0)) > int(time.time()):  # Check if not expired
            return self._decompress(bytes(item["value"]))
        return None

    def set(self, key: str, value: str, ttl: int = DEFAULT_TTL_SECONDS) -> None:
        expiration_time = int(time.time()) + ttl
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
