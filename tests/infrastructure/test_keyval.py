from hyperion.infrastructure.keyval import InMemoryStore


def test_in_memory_store_set_and_get() -> None:
    store = InMemoryStore()
    store.set("key1", "value1")
    assert store.get("key1") == "value1"


def test_in_memory_store_get_nonexistent_key() -> None:
    store = InMemoryStore()
    assert store.get("nonexistent") is None


def test_in_memory_store_delete() -> None:
    store = InMemoryStore()
    store.set("key1", "value1")
    store.delete("key1")
    assert store.get("key1") is None


def test_in_memory_store_keys() -> None:
    store = InMemoryStore()
    store.set("key1", "value1")
    store.set("key2", "value2")
    keys = list(store.keys())
    assert "key1" in keys
    assert "key2" in keys


def test_in_memory_store_exists() -> None:
    store = InMemoryStore()
    store.set("key1", "value1")
    assert store.exists("key1")
    assert not store.exists("key2")


def test_in_memory_store_compression_snappy() -> None:
    store = InMemoryStore(compression="snappy")
    store.set("key1", "value1")
    assert store.get("key1") == "value1"


def test_in_memory_store_compression_gzip() -> None:
    store = InMemoryStore(compression="gzip")
    store.set("key1", "value1")
    assert store.get("key1") == "value1"


def test_in_memory_store_iter() -> None:
    store = InMemoryStore()
    store.set("key1", "value1")
    store.set("key2", "value2")
    keys = list(iter(store))
    assert "key1" in keys
    assert "key2" in keys
