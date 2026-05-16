"""In-memory :class:`StoragePort` adapter (lite -- for tests and offline use)."""

from __future__ import annotations

import datetime
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from io import BytesIO
from typing import IO

from hyperion.log import get_logger
from hyperion.ports.storage import ObjectAttributes, ObjectNotFoundError

logger = get_logger("adapters.storage.memory")


def _to_bytes(data: bytes | IO[bytes]) -> bytes:
    return data if isinstance(data, bytes) else data.read()


class MemoryStorage:
    """A dict-of-bytes object store. Holds everything in process memory."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._mtimes: dict[str, datetime.datetime] = {}

    def put(self, key: str, data: bytes | IO[bytes]) -> None:
        logger.debug("Storing object in memory.", key=key)
        self._store[key] = _to_bytes(data)
        self._mtimes[key] = datetime.datetime.now(datetime.timezone.utc)

    async def put_async(self, key: str, data: bytes | IO[bytes]) -> None:
        self.put(key, data)

    def get(self, key: str) -> bytes:
        try:
            return self._store[key]
        except KeyError as error:
            raise ObjectNotFoundError(key) from error

    @contextmanager
    def open(self, key: str) -> Iterator[IO[bytes]]:
        with BytesIO(self.get(key)) as stream:
            yield stream

    def iter_keys(self, prefix: str) -> Iterator[str]:
        yield from (key for key in self._store if key.startswith(prefix))

    def exists(self, key: str) -> bool:
        return key in self._store

    def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._mtimes.pop(key, None)

    def get_attributes(self, key: str) -> ObjectAttributes:
        try:
            content = self._store[key]
        except KeyError as error:
            raise ObjectNotFoundError(key) from error
        return ObjectAttributes(
            etag=hashlib.md5(content, usedforsecurity=False).hexdigest(),
            size=len(content),
            last_modified=self._mtimes[key],
        )
