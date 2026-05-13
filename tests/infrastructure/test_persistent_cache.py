"""Tests for `hyperion.infrastructure.cache.PersistentCache`.

F3 of the DDD refactor deletes `PersistentCache` and replaces it with a
`KeyValueStore`-backed dict. Before that, lock the load/save lifecycle and the
on-disk row shape so the replacement can be checked for byte-level parity.
"""

from collections.abc import Iterable, Iterator
from typing import Any

import pytest

from hyperion.catalog.catalog import AssetNotFoundError, Catalog
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.infrastructure.cache import PersistentCache


class FakeCatalog:
    """Dict-backed catalog implementing just the surface PersistentCache uses."""

    def __init__(self, *, preload: Iterable[dict[str, Any]] | None = None) -> None:
        self.stored: dict[str, list[dict[str, Any]]] = {}
        self.store_calls = 0
        self.retrieve_calls = 0
        self._preload = list(preload) if preload is not None else None

    def retrieve_asset(self, asset: PersistentStoreAsset) -> Iterator[dict[str, Any]]:
        self.retrieve_calls += 1
        path = asset.get_path()
        if path in self.stored:
            yield from self.stored[path]
            return
        if self._preload is not None:
            self.stored[path] = self._preload
            yield from self._preload
            self._preload = None
            return
        raise AssetNotFoundError(asset)

    def store_asset(
        self,
        asset: PersistentStoreAsset,
        data: Iterable[dict[str, Any]],
        notify: bool = True,
        schema_path: str | None = None,
    ) -> None:
        self.store_calls += 1
        self.stored[asset.get_path()] = list(data)


@pytest.fixture
def asset() -> PersistentStoreAsset:
    return PersistentStoreAsset("TestStore", schema_version=1)


def test_init_requires_asset() -> None:
    with pytest.raises(ValueError, match="No asset provided"):
        PersistentCache("test")


def test_load_from_existing_asset(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog(
        preload=[
            {"key": "test:k1", "value": "v1"},
            {"key": "test:k2", "value": "v2"},
        ]
    )
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        assert cache.get("k1") == "v1"
        assert cache.get("k2") == "v2"
        assert catalog.retrieve_calls == 1


def test_load_missing_asset_starts_empty(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog()  # no preload → retrieve raises AssetNotFoundError
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        assert cache.get("missing") is None


def test_get_outside_context_raises(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog()
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="context manager"):
        cache.get("anything")


def test_exit_writes_dict_back_to_catalog(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog()
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        cache.set("k1", "v1")
        cache.set("k2", "v2")
    assert catalog.store_calls == 1
    stored = catalog.stored[asset.get_path()]
    # Persisted row shape: {"key", "value", "timestamp"}.
    assert sorted(row["key"] for row in stored) == ["test:k1", "test:k2"]
    assert set(stored[0].keys()) == {"key", "value", "timestamp"}


def test_roundtrip_through_catalog(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog()
    first = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with first:
        first.set("k", "value-1")

    second = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with second:
        assert second.get("k") == "value-1"


def test_unhashed_key_roundtrip(asset: PersistentStoreAsset) -> None:
    # GoogleMaps uses hash_keys=False. The key written to storage is the prefixed
    # original key, not a hash digest.
    catalog = FakeCatalog()
    cache = PersistentCache("gmaps", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        cache.set("Paris", "loc-data")
    stored = catalog.stored[asset.get_path()]
    assert stored[0]["key"] == "gmaps:Paris"
    assert stored[0]["value"] == "loc-data"


def test_hashed_key_uses_sha256_in_storage(asset: PersistentStoreAsset) -> None:
    import hashlib

    catalog = FakeCatalog()
    cache = PersistentCache("test", hash_keys=True, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        cache.set("Paris", "loc-data")
    stored = catalog.stored[asset.get_path()]
    expected = "test:" + hashlib.sha256(b"Paris").hexdigest()
    assert stored[0]["key"] == expected


def test_hit_and_delete(asset: PersistentStoreAsset) -> None:
    catalog = FakeCatalog()
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        cache.set("k", "v")
        assert cache.hit("k")
        cache.delete("k")
        assert not cache.hit("k")


def test_bytes_roundtrip_via_base64(asset: PersistentStoreAsset) -> None:
    # PersistentCache stores bytes as base64-encoded strings (parity with InMemoryCache).
    catalog = FakeCatalog()
    cache = PersistentCache("test", hash_keys=False, asset=asset, catalog=catalog)  # type: ignore[arg-type]
    with cache:
        cache.set_bytes("k", b"binary-payload")
        assert cache.get_bytes("k") == b"binary-payload"


def test_default_catalog_when_none(monkeypatch: pytest.MonkeyPatch, asset: PersistentStoreAsset) -> None:
    """PersistentCache(catalog=None) should fall back to Catalog.from_config()."""
    sentinel: Any = FakeCatalog()
    monkeypatch.setattr(Catalog, "from_config", classmethod(lambda cls: sentinel))
    cache = PersistentCache("test", hash_keys=False, asset=asset)
    assert cache.catalog is sentinel
