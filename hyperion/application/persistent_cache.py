"""Catalog-backed :class:`Cache` (relocated from ``infrastructure.cache``).

.. deprecated::
    ``PersistentCache`` is the ``Catalog`` <-> cache knot (F3 / Step 7). It is
    kept functional and importable for the whole ``hyperion-sdk`` 1.x line, but
    is deprecated and **removed in 2.0**. Its sole consumer was
    :class:`~hyperion.adapters.geocoder.google.GoogleMaps`, which now accepts an
    injected :class:`~hyperion.ports.keyval.KeyValueStore`
    (``hyperion.adapters.keyval.*``) instead -- prefer a ``KeyValueStore``
    adapter. The deprecated ``hyperion.infrastructure.cache.PersistentCache``
    alias resolves here lazily, so importing the cache shim no longer pulls
    ``Catalog`` / ``fastavro`` / boto3.
"""

from __future__ import annotations

from base64 import b64decode, b64encode
from typing import Any

from hyperion.catalog import AssetNotFoundError, Catalog
from hyperion.dateutils import utcnow
from hyperion.domain.assets import PersistentStoreAsset
from hyperion.log import get_logger
from hyperion.ports.cache import DEFAULT_TTL_SECONDS, Cache, CachingError

logger = get_logger("cache")


class PersistentCache(Cache):
    """Uses a persistent store asset to store the cached data.

    Persistent store asset is a key-value store that is stored in the catalog.
    Please note that for now, there is no locking mechanism in place and two services using the same cache
    may overwrite each other's data.

    Note that `default_ttl` and all ttl-related arguments are ignored.

    .. deprecated::
        Deprecated and removed in hyperion-sdk 2.0. Inject a
        :class:`~hyperion.ports.keyval.KeyValueStore` instead.
    """

    # TODO: Implement a locking or ownership mechanism
    # https://github.com/Zephyr-Trade/FVE-map/issues/11
    def __init__(
        self,
        prefix: str,
        hash_keys: bool = True,
        default_ttl: int = DEFAULT_TTL_SECONDS,
        asset: PersistentStoreAsset | None = None,
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
            raise CachingError(f"Failed to decode cached key {key!r}, make sure it was stored as bytes.") from error

    def _delete(self, key: str) -> None:
        cache_key = self._key(key)
        if cache_key in self.data:
            del self.data[cache_key]

    def _clear(self) -> None:
        self.data.clear()

    def _hit(self, key: str) -> bool:
        return self._key(key) in self.data
