"""Port: a serverless cache for our shenanigans.

Abstract :class:`Cache` base plus its contract types (:class:`CacheStats`,
:class:`CachingError`). Concrete adapters (``InMemoryCache``, ``LocalFileCache``,
``DynamoDBCache``) live in ``hyperion.adapters.cache.*``; ``Cache.from_config``
delegates backend selection to :mod:`hyperion.composition` (the single
composition root). The deprecated ``PersistentCache`` (the ``Catalog`` knot,
S7) moved to :mod:`hyperion.application.persistent_cache`.
"""

import hashlib
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from io import BytesIO, StringIO
from typing import IO, Any, ClassVar, Literal, cast, overload

try:
    import snappy
except ImportError:  # pragma: no cover - snappy is the optional [snappy] extra
    snappy = None

from hyperion.config import storage_config
from hyperion.log import get_logger

DEFAULT_TTL_SECONDS = 60

_SNAPPY_MISSING_HINT = (
    "snappy compression requires the optional 'snappy' extra. "
    "Install it with: pip install 'hyperion-sdk[snappy]'."
)

CacheKeyOpenMode = Literal["str", "bytes"]

logger = get_logger("cache")


@dataclass
class CacheStats:
    hits: int
    misses: int
    gets: int
    deletes: int
    clears: int
    sets: int

    @classmethod
    def empty(cls) -> "CacheStats":
        return cls(hits=0, misses=0, gets=0, deletes=0, clears=0, sets=0)

    def __sub__(self, other: Any) -> dict[str, int]:
        if not isinstance(other, CacheStats):
            return NotImplemented
        difference: dict[str, int] = {}
        for field in CacheStats.__dataclass_fields__:
            ours = getattr(self, field)
            theirs = getattr(other, field)
            if not isinstance(ours, int):
                raise TypeError(f"Field {field!r} is not an integer on {self!r}.")
            if not isinstance(theirs, int):
                raise TypeError(f"Field {field!r} is not an integer on {other!r}.")
            if diff := ours - theirs:
                difference[field] = diff
        return difference


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
        from hyperion import composition

        instance_key = (storage_config.cache_key_prefix, True)
        if instance_key not in Cache._instances:
            cls._instances[instance_key] = composition.default_cache()
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
        self._stats = CacheStats.empty()

    def reset_stats(self) -> None:
        self._stats = CacheStats.empty()

    @property
    def stats(self) -> CacheStats:
        return replace(self._stats)

    def _key(self, key: str) -> str:
        """Generates a cache key from a given key."""
        if self.hash_keys:
            key = hashlib.sha256(key.encode(encoding="utf-8")).hexdigest()
        prefix = f"{self.prefix}:" if self.prefix else ""
        return f"{prefix}{key}"

    def _compress(self, value: str) -> bytes:
        """Compresses a value using snappy compression."""
        return self._compress_bytes(value.encode(encoding="utf-8"))

    def _compress_bytes(self, value: bytes) -> bytes:
        """Compress a bytes value using snappy compression."""
        if snappy is None:
            raise ImportError(_SNAPPY_MISSING_HINT)
        compressed_value = snappy.compress(value)
        logger.debug(
            "Compressed value using snappy compression.",
            original_length=len(value),
            compressed_length=len(compressed_value),
            ratio=len(compressed_value) / len(value),
        )
        return cast(bytes, compressed_value)

    def _decompress(self, value: bytes) -> str:
        """Decompresses a value using snappy decompression."""
        return self._decompress_bytes(value).decode(encoding="utf-8")

    def _decompress_bytes(self, value: bytes) -> bytes:
        """Decompresses a bytes value using snappy decompression."""
        if snappy is None:
            raise ImportError(_SNAPPY_MISSING_HINT)
        return cast(bytes, snappy.decompress(value))

    @overload
    @contextmanager
    def _open(self, key: str, mode: Literal["str"]) -> Iterator[IO[str]]:
        pass

    @overload
    @contextmanager
    def _open(self, key: str, mode: Literal["bytes"]) -> Iterator[IO[bytes]]:
        pass

    @contextmanager
    def _open(self, key: str, mode: CacheKeyOpenMode = "str") -> Iterator[IO[bytes] | IO[str]]:
        """Opens a file-like object for reading and writing to the cache.
        This is useful for streaming data to and from the cache.
        In the base class, this has no performance benefits, because the implementation is in memory (using
        StringIO or BytesIO), but in the LocalFileCache, this is useful for streaming data to and from the file system.

        Args:
            key (str): The key for the cache.
            mode (str): The mode for opening the file-like object. Can be "str" or "bytes".

        Yields:
            IO[bytes] | IO[str]: A file-like object for reading and writing to the cache.
        """
        if mode == "str":
            with StringIO(self._get(key) or "") as file:
                yield file
                self._set(key, file.getvalue())
        elif mode == "bytes":
            with BytesIO(self._get_bytes(key) or b"") as file:
                yield file
                self._set_bytes(key, file.getvalue())
        else:
            raise ValueError(f"Unsupported open mode {mode!r} - 'str' or 'bytes' are supported.")

    @overload
    @contextmanager
    def open(self, key: str, mode: Literal["str"]) -> Iterator[IO[str]]:
        pass

    @overload
    @contextmanager
    def open(self, key: str, mode: Literal["bytes"]) -> Iterator[IO[bytes]]:
        pass

    @contextmanager
    def open(self, key: str, mode: CacheKeyOpenMode = "str") -> Iterator[IO[bytes] | IO[str]]:
        self._stats.gets += 1
        with self._open(key, mode) as file:
            yield file
        self._stats.sets += 1

    @abstractmethod
    def _get(self, key: str) -> str | None:
        pass

    def get(self, key: str) -> str | None:
        """Gets a value from the cache."""
        self._stats.gets += 1
        if (cached := self._get(key)) is not None:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        return cached

    @abstractmethod
    def _set(self, key: str, value: str) -> None:
        pass

    def set(self, key: str, value: str) -> None:
        """Sets a value in the cache."""
        self._stats.sets += 1
        self._set(key, value)

    @abstractmethod
    def _get_bytes(self, key: str) -> bytes | None:
        pass

    def get_bytes(self, key: str) -> bytes | None:
        """Gets a bytes value from the cache."""
        self._stats.gets += 1
        if (cached := self._get_bytes(key)) is not None:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        return cached

    @abstractmethod
    def _set_bytes(self, key: str, value: bytes) -> None:
        pass

    def set_bytes(self, key: str, value: bytes) -> None:
        """Sets a bytes value in the cache."""
        self._stats.sets += 1
        self._set_bytes(key, value)

    @abstractmethod
    def _delete(self, key: str) -> None:
        pass

    def delete(self, key: str) -> None:
        """Deletes a value from the cache."""
        self._stats.deletes += 1
        self._delete(key)

    @abstractmethod
    def _clear(self) -> None:
        pass

    def clear(self) -> None:
        """Clears the cache."""
        self._stats.clears += 1
        self._clear()

    @abstractmethod
    def _hit(self, key: str) -> bool:
        pass

    def hit(self, key: str) -> bool:
        """Checks if a key exists in the cache."""
        return self._hit(key)
