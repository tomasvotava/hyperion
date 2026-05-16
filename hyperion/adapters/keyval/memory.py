"""In-memory :class:`KeyValueStore` adapter (lite -- not persistent)."""

from __future__ import annotations

from collections.abc import Iterable

from hyperion.ports.keyval import CompressionType, KeyValueStore


class InMemoryStore(KeyValueStore):
    """An in-memory key-value store.

    This store is not persistent and is not shared between instances.
    """

    def __init__(self, prefix: str | None = None, compression: CompressionType | None = None):
        super().__init__(prefix, compression)
        self.store: dict[str, bytes] = {}

    def _get_raw(self, hashed_key: str) -> bytes | None:
        return self.store.get(hashed_key)

    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        self.store[hashed_key] = compresed_value

    def _delete_raw(self, hashed_key: str) -> None:
        self.store.pop(hashed_key, None)

    def _iter_all_keys(self) -> Iterable[str]:
        yield from self.store
