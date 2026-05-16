"""Port: object storage abstraction.

``StoragePort`` is the dependency-inversion seam between :class:`Catalog`-style
callers and a concrete object store (S3, local filesystem, in-memory). It is a
``runtime_checkable`` :class:`typing.Protocol` -- adapters satisfy it
structurally, no forced inheritance (see ``docs/ddd-refactor-plan.md`` F1 /
Step 4).

Contract:

* ``get`` / ``open`` / ``get_attributes`` raise :class:`ObjectNotFoundError`
  when the key is absent.
* ``delete`` is idempotent -- deleting a missing key is a no-op, never an error.
* ``iter_keys`` yields keys in the same namespace ``put`` accepts (adapters that
  add a storage-side prefix strip it back off).
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import IO, Protocol, runtime_checkable


class ObjectNotFoundError(KeyError):
    """Raised when a requested storage key does not exist.

    Subclasses :class:`KeyError` so callers that already ``except KeyError``
    (and the in-memory adapter's natural error) keep working.
    """


@dataclass(frozen=True)
class ObjectAttributes:
    """Backend-agnostic metadata for a stored object."""

    etag: str
    size: int
    last_modified: datetime.datetime


@runtime_checkable
class StoragePort(Protocol):
    """Abstraction over object storage backends."""

    def put(self, key: str, data: bytes | IO[bytes]) -> None:
        """Store ``data`` under ``key``, overwriting any existing object."""
        ...

    async def put_async(self, key: str, data: bytes | IO[bytes]) -> None:
        """Asynchronously store ``data`` under ``key``."""
        ...

    def get(self, key: str) -> bytes:
        """Return the full object stored under ``key``.

        Raises:
            ObjectNotFoundError: if ``key`` does not exist.
        """
        ...

    def open(self, key: str) -> AbstractContextManager[IO[bytes]]:
        """Open ``key`` for streaming binary reads.

        Raises:
            ObjectNotFoundError: if ``key`` does not exist.
        """
        ...

    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Yield every key whose name starts with ``prefix``."""
        ...

    def exists(self, key: str) -> bool:
        """Return whether an object is stored under ``key``."""
        ...

    def delete(self, key: str) -> None:
        """Delete ``key``. Idempotent -- a missing key is not an error."""
        ...

    def get_attributes(self, key: str) -> ObjectAttributes:
        """Return :class:`ObjectAttributes` for ``key``.

        Raises:
            ObjectNotFoundError: if ``key`` does not exist.
        """
        ...
