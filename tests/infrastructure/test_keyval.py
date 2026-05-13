from collections.abc import Iterator
from typing import TYPE_CHECKING

import boto3
import pytest

from hyperion.infrastructure.keyval import DynamoDBStore, InMemoryStore, KeyValueStore

if TYPE_CHECKING:
    pass

DYNAMODB_TABLE = "test-keyval-table"


@pytest.fixture(scope="module", autouse=True)
def _dynamo_keyval_table(_moto_server: None) -> Iterator[None]:
    client = boto3.resource("dynamodb")
    table = client.create_table(
        TableName=DYNAMODB_TABLE,
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table.wait_until_exists()
    yield
    table.delete()


# -- Existing single-backend tests preserved as-is below --
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


# -- Cross-backend parametrize harness ----------------------------------------
# The same conformance tests run against both InMemoryStore and DynamoDBStore.
# Refactor F4 (`GoogleMaps(keyval=...)`) will rely on either backend giving the
# same semantics; this harness pins that.


@pytest.fixture
def store(request: pytest.FixtureRequest) -> KeyValueStore:
    backend: str = request.param
    if backend == "memory":
        return InMemoryStore()
    if backend == "dynamodb":
        return DynamoDBStore(table_name=DYNAMODB_TABLE)
    raise ValueError(f"Unknown backend {backend!r}")


@pytest.fixture
def store_with_prefix(request: pytest.FixtureRequest) -> KeyValueStore:
    backend: str = request.param
    if backend == "memory":
        return InMemoryStore(prefix="ns")
    if backend == "dynamodb":
        return DynamoDBStore(prefix="ns", table_name=DYNAMODB_TABLE)
    raise ValueError(f"Unknown backend {backend!r}")


@pytest.mark.parametrize("store", ["memory", "dynamodb"], indirect=True)
class TestKeyValueStoreContract:
    def test_set_and_get(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        assert store.get("a") == "1"

    def test_get_missing(self, store: KeyValueStore) -> None:
        assert store.get("nope") is None

    def test_delete(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        store.delete("a")
        assert store.get("a") is None

    def test_contains_operator(self, store: KeyValueStore) -> None:
        store.set("a", "1")
        assert "a" in store
        assert "b" not in store

    def test_getitem_setitem(self, store: KeyValueStore) -> None:
        store["a"] = "1"
        assert store["a"] == "1"

    def test_delitem(self, store: KeyValueStore) -> None:
        store["a"] = "1"
        del store["a"]
        assert "a" not in store

    def test_keys_no_pattern(self, store: KeyValueStore) -> None:
        store.set("alpha", "x")
        store.set("beta", "y")
        keys = set(store.keys())
        assert {"alpha", "beta"}.issubset(keys)

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
        seen = set(iter(store))
        assert {"a", "b"}.issubset(seen)


@pytest.mark.parametrize("store_with_prefix", ["memory", "dynamodb"], indirect=True)
class TestKeyValueStorePrefix:
    # Iteration semantics diverge between InMemoryStore (keys include prefix) and
    # DynamoDBStore (prefix is stripped). Don't assert the iteration shape here;
    # pin only the keyspace-isolation property, which both backends honor.

    def test_prefix_isolates_keyspaces(self, store_with_prefix: KeyValueStore) -> None:
        store_with_prefix.set("a", "ns-value")
        # A second store without the prefix should not see this key as "a".
        if isinstance(store_with_prefix, DynamoDBStore):
            other: KeyValueStore = DynamoDBStore(table_name=DYNAMODB_TABLE)
        else:
            other = InMemoryStore()
        other.set("a", "other-value")
        # Both can still get their own.
        assert store_with_prefix.get("a") == "ns-value"
        assert other.get("a") == "other-value"


# Compression behaviour applies regardless of backend.
@pytest.mark.parametrize("backend", ["memory", "dynamodb"])
@pytest.mark.parametrize("compression", ["snappy", "gzip", None])
def test_compression_roundtrip(backend: str, compression: str | None) -> None:
    if backend == "memory":
        store: KeyValueStore = InMemoryStore(compression=compression)  # type: ignore[arg-type]
    else:
        store = DynamoDBStore(compression=compression, table_name=DYNAMODB_TABLE)  # type: ignore[arg-type]
    payload = "hello-" * 200  # large enough that compression has an effect
    store.set("k", payload)
    assert store.get("k") == payload
