"""Unit tests for the new lite ``FilesystemStore`` keyval adapter.

Mirrors the cross-backend contract of ``tests/infrastructure/test_keyval.py``
(directory-of-files backing, atomic writes, prefix-stripping iteration).
"""

from pathlib import Path

import pytest

from hyperion.adapters.keyval.filesystem import FilesystemStore
from hyperion.ports.keyval import KeyValueStore


@pytest.fixture
def store(tmp_path: Path) -> KeyValueStore:
    return FilesystemStore(tmp_path / "kv")


def test_creates_root_directory(tmp_path: Path) -> None:
    root = tmp_path / "nested" / "kv"
    FilesystemStore(root)
    assert root.is_dir()


def test_rejects_non_directory_root(tmp_path: Path) -> None:
    file_path = tmp_path / "afile"
    file_path.write_text("x")
    with pytest.raises(ValueError, match="is not a directory"):
        FilesystemStore(file_path)


class TestFilesystemStoreContract:
    def test_set_and_get(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        assert store.get("a") == "1"

    def test_get_missing(self, store: KeyValueStore) -> None:
        assert store.get("nope") is None

    def test_set_overwrites(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        store.set("a", "2")
        assert store.get("a") == "2"

    def test_delete(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        store.delete("a")
        assert store.get("a") is None

    def test_delete_missing_is_noop(self, store: KeyValueStore) -> None:
        store.delete("never-set")

    def test_contains_operator(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        assert "a" in store
        assert "b" not in store

    def test_getitem_setitem_delitem(self, store: KeyValueStore) -> None:
        store["a"] = "1"
        assert store["a"] == "1"
        del store["a"]
        assert "a" not in store

    def test_keys_fnmatch_glob(self, store: KeyValueStore) -> None:
        store.set("alpha", "x")
        store.set("beta", "y")
        store.set("apple", "z")
        keys = set(store.keys(match="a*"))
        assert "alpha" in keys
        assert "apple" in keys
        assert "beta" not in keys

    def test_iter_yields_all(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        store.set("b", "2")
        assert {"a", "b"}.issubset(set(iter(store)))

    def test_keys_with_filesystem_unsafe_characters(self, store: KeyValueStore) -> None:
        # url-safe-base64 filename encoding must round-trip arbitrary keys.
        key = "ns/sub:weird key"
        store.set(key, "v")
        assert store.get(key) == "v"
        assert key in set(iter(store))


def test_prefix_strips_on_iteration(tmp_path: Path) -> None:
    store = FilesystemStore(tmp_path / "kv", prefix="ns")
    store.set("a", "ns-value")
    assert list(iter(store)) == ["a"]
    assert store.get("a") == "ns-value"


def test_prefix_isolates_keyspace(tmp_path: Path) -> None:
    prefixed = FilesystemStore(tmp_path / "kv", prefix="ns")
    plain = FilesystemStore(tmp_path / "kv")
    prefixed.set("a", "ns-value")
    plain.set("a", "other-value")
    assert prefixed.get("a") == "ns-value"
    assert plain.get("a") == "other-value"


def test_persists_across_instances(tmp_path: Path) -> None:
    root = tmp_path / "kv"
    FilesystemStore(root).set("k", "persisted")
    assert FilesystemStore(root).get("k") == "persisted"


@pytest.mark.parametrize("compression", ["snappy", "gzip", None])
def test_compression_roundtrip(tmp_path: Path, compression: str | None) -> None:
    store = FilesystemStore(tmp_path / "kv", compression=compression)  # type: ignore[arg-type]
    payload = "hello-" * 200
    store.set("k", payload)
    assert store.get("k") == payload
