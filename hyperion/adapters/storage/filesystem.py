"""Filesystem :class:`StoragePort` adapter (lite -- restores local-path Catalog).

Keys are interpreted as POSIX-style paths relative to a single root directory.
This is the adapter that makes ``Catalog(storage=FilesystemStorage(...))``
possible once S5 inverts Catalog onto :class:`StoragePort`.
"""

from __future__ import annotations

import datetime
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO

from hyperion.log import get_logger
from hyperion.ports.storage import ObjectAttributes, ObjectNotFoundError

logger = get_logger("adapters.storage.filesystem")


def _to_bytes(data: bytes | IO[bytes]) -> bytes:
    return data if isinstance(data, bytes) else data.read()


class FilesystemStorage:
    """A :class:`StoragePort` backed by a directory tree under ``root``."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()

    def _resolve(self, key: str) -> Path:
        target = (self._root / key).resolve()
        if target != self._root and not target.is_relative_to(self._root):
            raise ValueError(f"Key {key!r} escapes the storage root.")
        return target

    def put(self, key: str, data: bytes | IO[bytes]) -> None:
        target = self._resolve(key)
        logger.debug("Writing object to disk.", key=key, path=target.as_posix())
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_to_bytes(data))

    async def put_async(self, key: str, data: bytes | IO[bytes]) -> None:
        self.put(key, data)

    def get(self, key: str) -> bytes:
        try:
            return self._resolve(key).read_bytes()
        except FileNotFoundError as error:
            raise ObjectNotFoundError(key) from error

    @contextmanager
    def open(self, key: str) -> Iterator[IO[bytes]]:
        try:
            handle = self._resolve(key).open("rb")
        except FileNotFoundError as error:
            raise ObjectNotFoundError(key) from error
        try:
            yield handle
        finally:
            handle.close()

    def iter_keys(self, prefix: str) -> Iterator[str]:
        for path in self._root.rglob("*"):
            if not path.is_file():
                continue
            key = path.relative_to(self._root).as_posix()
            if key.startswith(prefix):
                yield key

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def delete(self, key: str) -> None:
        self._resolve(key).unlink(missing_ok=True)

    def get_attributes(self, key: str) -> ObjectAttributes:
        target = self._resolve(key)
        try:
            stat = target.stat()
        except FileNotFoundError as error:
            raise ObjectNotFoundError(key) from error
        return ObjectAttributes(
            etag=hashlib.md5(target.read_bytes(), usedforsecurity=False).hexdigest(),
            size=stat.st_size,
            last_modified=datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.UTC),
        )
