"""Deprecated import shim for cache adapters (still hosts ``PersistentCache``).

.. deprecated::
    The abstract :class:`Cache`, :class:`CacheStats` and :class:`CachingError`
    moved to :mod:`hyperion.ports.cache`. The concrete adapters moved to
    ``hyperion.adapters.cache.*`` (``InMemoryCache`` ->
    :mod:`hyperion.adapters.cache.memory`, ``LocalFileCache`` ->
    :mod:`hyperion.adapters.cache.filesystem`, ``DynamoDBCache`` ->
    :mod:`hyperion.adapters.cache.dynamodb`). Import them from there. This
    module keeps every relocated symbol importable (with a
    :class:`DeprecationWarning`, resolved lazily so the import does not pull
    boto3) for the whole ``hyperion-sdk`` 1.x line; symbols are removed in 2.0.

    :class:`PersistentCache` is *not* relocated here -- it is the
    ``Catalog`` <-> cache knot removed in step S7 and still lives in this
    module until then.
"""

import importlib
from base64 import b64decode, b64encode
from typing import TYPE_CHECKING, Any

from hyperion._compat import moved_attr
from hyperion.catalog import AssetNotFoundError, Catalog
from hyperion.dateutils import utcnow
from hyperion.domain.assets import PersistentStoreAsset
from hyperion.log import get_logger
from hyperion.ports.cache import DEFAULT_TTL_SECONDS, CacheKeyOpenMode
from hyperion.ports.cache import Cache as _Cache
from hyperion.ports.cache import CacheStats as _CacheStats
from hyperion.ports.cache import CachingError as _CachingError

if TYPE_CHECKING:
    from hyperion.adapters.cache.dynamodb import DYNAMODB_MAX_LENGTH, DynamoDBCache
    from hyperion.adapters.cache.filesystem import DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE, LocalFileCache
    from hyperion.adapters.cache.memory import InMemoryCache
    from hyperion.ports.cache import Cache, CacheStats, CachingError

logger = get_logger("cache")

_OLD_MODULE = "hyperion.infrastructure.cache"

# Abstract contract types -- already imported from the lite ``ports`` layer.
_MOVED: dict[str, tuple[object, str]] = {
    "Cache": (_Cache, "hyperion.ports.cache"),
    "CacheStats": (_CacheStats, "hyperion.ports.cache"),
    "CachingError": (_CachingError, "hyperion.ports.cache"),
}

# Concrete adapters + their constants -- resolved lazily so importing this
# shim never pulls boto3 (the DynamoDB adapter).
_MOVED_LAZY: dict[str, str] = {
    "InMemoryCache": "hyperion.adapters.cache.memory",
    "LocalFileCache": "hyperion.adapters.cache.filesystem",
    "DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE": "hyperion.adapters.cache.filesystem",
    "DynamoDBCache": "hyperion.adapters.cache.dynamodb",
    "DYNAMODB_MAX_LENGTH": "hyperion.adapters.cache.dynamodb",
}

__all__ = [
    "DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE",
    "DEFAULT_TTL_SECONDS",
    "DYNAMODB_MAX_LENGTH",
    "Cache",
    "CacheKeyOpenMode",
    "CacheStats",
    "CachingError",
    "DynamoDBCache",
    "InMemoryCache",
    "LocalFileCache",
    "PersistentCache",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        value, new_module = _MOVED[name]
        return moved_attr(name=name, value=value, old_module=_OLD_MODULE, new_module=new_module)
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class PersistentCache(_Cache):
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

    def _get(self, key: str) -> str | None:
        cache_key = self._key(key)
        return self.data.get(cache_key)

    @property
    def data(self) -> dict[str, str]:
        if self._data is None:
            raise RuntimeError("Persistent cache must be used as a context manager.")
        return self._data

    def _set(self, key: str, value: str) -> None:
        cache_key = self._key(key)
        self.data[cache_key] = value

    def _set_bytes(self, key: str, value: bytes) -> None:
        return self._set(key, b64encode(value).decode("ascii"))

    def _get_bytes(self, key: str) -> bytes | None:
        if (cached := self._get(key)) is None:
            return None
        try:
            return b64decode(cached.encode("ascii"))
        except Exception as error:
            raise _CachingError(f"Failed to decode cached key {key!r}, make sure it was stored as bytes.") from error

    def _delete(self, key: str) -> None:
        cache_key = self._key(key)
        if cache_key in self.data:
            del self.data[cache_key]

    def _clear(self) -> None:
        self.data.clear()

    def _hit(self, key: str) -> bool:
        return self._key(key) in self.data
