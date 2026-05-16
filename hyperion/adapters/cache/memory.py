"""In-memory :class:`Cache` adapter (lite -- cachetools-backed)."""

from __future__ import annotations

from base64 import b64decode, b64encode

import cachetools

from hyperion.ports.cache import DEFAULT_TTL_SECONDS, Cache, CachingError


class InMemoryCache(Cache):
    """An in-memory cache for our shenanigans."""

    MAX_KEYS = 1000
    cache: cachetools.TTLCache[str, str]

    def __init__(
        self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS, max_size: int = MAX_KEYS
    ):
        super().__init__(prefix, hash_keys, default_ttl)
        self.max_size = max_size
        self.cache = cachetools.TTLCache[str, str](maxsize=self.max_size, ttl=self.default_ttl)

    def _get(self, key: str) -> str | None:
        return self.cache.get(self._key(key))

    def _set(self, key: str, value: str) -> None:
        self.cache[self._key(key)] = value

    def _set_bytes(self, key: str, value: bytes) -> None:
        self.cache[self._key(key)] = b64encode(value).decode("ascii")

    def _get_bytes(self, key: str) -> bytes | None:
        if (cached := self.cache.get(self._key(key))) is None:
            return None
        try:
            return b64decode(cached.encode("ascii"))
        except Exception as error:
            raise CachingError(f"Failed to decode cached key {key!r}, make sure it was stored as bytes.") from error

    def _delete(self, key: str) -> None:
        self.cache.pop(self._key(key), None)

    def _clear(self) -> None:
        self.cache.clear()

    def _hit(self, key: str) -> bool:
        return self._key(key) in self.cache
