"""Filesystem-backed :class:`Cache` adapter (lite -- stdlib only).

``snappy`` compression is delegated to the abstract base; it degrades
gracefully when ``python-snappy`` is not installed.
"""

from __future__ import annotations

import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Literal, overload

from hyperion.log import get_logger
from hyperion.ports.cache import DEFAULT_TTL_SECONDS, Cache, CacheKeyOpenMode

DEFAULT_LOCAL_FILE_CACHE_DIRNAME = "hyperion-cache"
DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE = 256 * (1024**2)

logger = get_logger("cache")


class LocalFileCache(Cache):
    """A local file cache for our shenanigans."""

    def __init__(
        self,
        prefix: str,
        hash_keys: bool = True,
        default_ttl: int = DEFAULT_TTL_SECONDS,
        root_path: Path | None = None,
        max_size: int | None = DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE,
        use_compression: bool = True,
    ) -> None:
        super().__init__(prefix, hash_keys, default_ttl)
        self.root_path = root_path or (Path(tempfile.gettempdir()) / DEFAULT_LOCAL_FILE_CACHE_DIRNAME)
        if self.root_path.exists() and not self.root_path.is_dir():
            raise ValueError(f"Given local cache path ({self.root_path.as_posix()}) is not a directory.")
        self.use_compression = use_compression
        self.max_size = max_size
        self._assert_root_path()
        logger.info(f"Initialized LocalFileCache in {self.root_path.as_posix()}.", root_path=self.root_path.as_posix())
        if not hash_keys:
            logger.warning("When using filesystem cache, it is recommended to hash keys.")

    def _assert_root_path(self) -> None:
        self.root_path.mkdir(parents=True, exist_ok=True)

    def _cleanup(self) -> None:
        """Clean up all expired files from the cache."""
        self._assert_root_path()
        for key_path in self.root_path.iterdir():
            if key_path.is_file() and self._is_expired(key_path):
                logger.debug("Cleaning up expired file.", key_path=key_path)
                key_path.unlink()

    def get_total_size(self) -> int:
        self._assert_root_path()
        return sum(key_path.stat().st_size for key_path in self.root_path.iterdir() if key_path.is_file())

    @overload
    @contextmanager
    def _open(self, key: str, mode: Literal["str"]) -> Iterator[IO[str]]:
        pass

    @overload
    @contextmanager
    def _open(self, key: str, mode: Literal["bytes"]) -> Iterator[IO[bytes]]:
        pass

    @contextmanager
    def _open(self, key: str, mode: CacheKeyOpenMode = "str") -> Iterator[IO[str] | IO[bytes]]:
        self.hit(key)  # this should expire the file if needed
        if self.use_compression:
            logger.warning(
                "Current LocalFileCache implementation does not work well with compression on. "
                "It compresses the content in-memory. For now, you should consider using "
                "use_compression=False and utilizing e.g. snappy or gzip manually."
            )
            with super()._open(key, mode) as file:
                yield file
            return
        key_path = self._key_path(key)
        key_path.touch(exist_ok=True)
        if mode == "str":
            with key_path.open("r+") as file:
                file.seek(0)
                yield file
                # r+ does not truncate; without this a shorter rewrite leaves
                # trailing bytes from the previous value.
                file.truncate(file.tell())
        elif mode == "bytes":
            with key_path.open("r+b") as file:
                file.seek(0)
                yield file
                file.truncate(file.tell())
        else:
            raise ValueError(f"Unsupported open mode {mode!r} - 'str' or 'bytes' are supported.")
        if not key_path.stat().st_size:
            key_path.unlink(missing_ok=True)

    def shrink_to_fit_max_size(self) -> None:
        self._cleanup()
        if not self.max_size or self.max_size < 0:
            return
        total_size = self.get_total_size()
        if total_size <= self.max_size:
            return
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

    def _get(self, key: str) -> str | None:
        if (cached := self._get_bytes(key)) is None:
            return None
        return cached.decode("utf-8")

    def _get_bytes(self, key: str) -> bytes | None:
        self._assert_root_path()
        if not self.hit(key):
            return None
        key_path = self._key_path(key)
        logger.debug("Reading key from file.", key=key, path=key_path.as_posix())
        content = key_path.read_bytes()
        if self.use_compression:
            return self._decompress_bytes(content)
        return content

    def _perform_set(self, key: str, value: str | bytes) -> None:
        self._assert_root_path()
        key_path = self._key_path(key)
        logger.debug("Storing key into a file.", key=key, path=key_path.as_posix())
        if isinstance(value, str):
            value = value.encode("utf-8")
        content = self._compress_bytes(value) if self.use_compression else value
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile("wb", dir=self.root_path, delete=False) as tmp_file:
                tmp_path = tmp_file.name
                tmp_file.write(content)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, key_path)  # noqa: PTH105 - atomic same-dir replace
            tmp_path = None
        finally:
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)
        self.shrink_to_fit_max_size()

    def _set(self, key: str, value: str) -> None:
        return self._perform_set(key, value)

    def _set_bytes(self, key: str, value: bytes) -> None:
        return self._perform_set(key, value)

    def _delete(self, key: str) -> None:
        self._assert_root_path()
        key_path = self._key_path(key)
        if not key_path.exists():
            return None
        logger.debug("Removing cached key.", key=key, path=key_path.as_posix())
        key_path.unlink()

    def _hit(self, key: str) -> bool:
        key_path = self._key_path(key)
        if not key_path.exists():
            return False
        if self._is_expired(key_path):
            logger.debug("Key is expired, deleting file.", key=key, key_path=key_path)
            key_path.unlink()
            return False
        return True

    def _clear(self) -> None:
        self._assert_root_path()
        for file in self.root_path.iterdir():
            if not file.is_file():
                continue
            logger.debug("Removing cached key.", path=file.as_posix())
            file.unlink()
