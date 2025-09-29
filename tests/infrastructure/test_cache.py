import os
from base64 import b64encode
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from time import sleep

import boto3
import pytest

from hyperion.infrastructure.cache import Cache, CacheStats, CachingError, DynamoDBCache, InMemoryCache, LocalFileCache

DYNAMODB_TABLE = "test-table"


@pytest.fixture(scope="module", autouse=True)
def _dynamo_table(_moto_server: None) -> Iterator[None]:
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


@pytest.fixture
def dynamodb_cache(_dynamo_table: None) -> DynamoDBCache:
    return DynamoDBCache(prefix="test", hash_keys=True, table_name=DYNAMODB_TABLE)


@pytest.fixture
def in_memory_cache() -> InMemoryCache:
    return InMemoryCache(prefix="test", hash_keys=True)


@pytest.fixture
def local_file_cache(tmp_path: Path) -> Iterator[LocalFileCache]:
    try:
        yield (cache := LocalFileCache(prefix="test", root_path=tmp_path))
    finally:
        cache.clear()


@pytest.fixture
def local_file_cache_no_compression(tmp_path: Path) -> Iterator[LocalFileCache]:
    try:
        yield (cache := LocalFileCache(prefix="test-no-compression", root_path=tmp_path, use_compression=False))
    finally:
        cache.clear()


@pytest.fixture
def local_file_cache_maxsize(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[LocalFileCache]:
    try:
        yield (cache := LocalFileCache(prefix="test-maxsize", root_path=tmp_path, max_size=request.param))
    finally:
        cache.clear()


@pytest.fixture
def local_file_cache_expire(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[LocalFileCache]:
    try:
        yield (cache := LocalFileCache(prefix="test-expire", root_path=tmp_path, default_ttl=request.param))
    finally:
        cache.clear()


@pytest.fixture
def instance(
    dynamodb_cache: DynamoDBCache,
    in_memory_cache: InMemoryCache,
    local_file_cache: LocalFileCache,
    local_file_cache_no_compression: LocalFileCache,
    request: pytest.FixtureRequest,
) -> Cache:
    return {
        "dynamodb_cache": dynamodb_cache,
        "in_memory_cache": in_memory_cache,
        "local_file_cache": local_file_cache,
        "local_file_cache_no_compression": local_file_cache_no_compression,
    }[request.param]


@pytest.mark.parametrize(
    "instance",
    ["dynamodb_cache", "in_memory_cache", "local_file_cache", "local_file_cache_no_compression"],
    indirect=True,
)
class TestCache:
    def test_store_and_retrieve(self, instance: Cache) -> None:
        assert instance.get("test") is None, "The cache must be empty before test"
        assert not instance.hit("test")
        instance.set("test", "this is a test value")
        assert instance.hit("test")
        assert instance.get("test") == "this is a test value"

        assert instance.get_bytes("test-bytes") is None, "The cache must be empty before test"
        assert not instance.hit("test-bytes")
        instance.set_bytes("test-bytes", b"bytes value")
        assert instance.hit("test-bytes")
        assert instance.get_bytes("test-bytes") == b"bytes value"

    def test_store_and_delete(self, instance: Cache) -> None:
        assert instance.get("delete") is None
        instance.set("delete", "will delete")
        assert instance.get("delete") == "will delete"
        instance.delete("delete")
        assert instance.get("delete") is None
        assert not instance.hit("delete")

    def test_clear(self, instance: Cache) -> None:
        test_data_str = {b64encode(os.urandom(16)).decode("ascii"): b64encode(os.urandom(64)).decode("ascii")}
        test_data_bytes = {b64encode(os.urandom(16)).decode("ascii"): b64encode(os.urandom(64))}

        for key, value_str in test_data_str.items():
            assert instance.get(key) is None
            assert not instance.hit(key)
            instance.set(key, value_str)
            assert instance.hit(key)
            assert instance.get(key) == value_str

        for key, value_bytes in test_data_bytes.items():
            assert instance.get_bytes(key) is None
            assert not instance.hit(key)
            instance.set_bytes(key, value_bytes)
            assert instance.hit(key)
            assert instance.get_bytes(key) == value_bytes

        instance.clear()

        for key in (*test_data_str.keys(), *test_data_bytes.keys()):
            assert not instance.hit(key)

    def test_open_str_new(self, instance: Cache) -> None:
        assert not instance.hit("file-str")
        with instance.open("file-str", "str") as file:
            assert file.tell() == 0
            assert file.read() == ""
            file.write("this is a test")
        assert instance.get("file-str") == "this is a test"

    def test_open_str_existing(self, instance: Cache) -> None:
        instance.set("file-str-existing", "this was already there")
        with instance.open("file-str-existing", "str") as file:
            assert file.read() == "this was already there"
            file.truncate(0)
            file.seek(0)
            file.write("this is a new content")
        assert instance.get("file-str-existing") == "this is a new content"

    def test_open_bytes_new(self, instance: Cache) -> None:
        assert not instance.hit("file-bytes")
        with instance.open("file-bytes", "bytes") as file:
            assert file.tell() == 0
            assert file.read() == b""
            file.write(b"this is a test")
        assert instance.get_bytes("file-bytes") == b"this is a test"

    def test_open_bytes_existing(self, instance: Cache) -> None:
        instance.set_bytes("file-bytes-existing", b"this was already there")
        with instance.open("file-bytes-existing", "bytes") as file:
            assert file.read() == b"this was already there"
            file.truncate(0)
            file.seek(0)
            file.write(b"this is a new content")
        assert instance.get_bytes("file-bytes-existing") == b"this is a new content"

    def test_mixed_access(self, instance: Cache) -> None:
        assert not instance.hit("mixed")
        instance.set("mixed", "is a string")
        if isinstance(instance, InMemoryCache):
            # InMemoryCache stores bytes as base64
            with pytest.raises(
                CachingError, match=r"Failed to decode cached key .mixed., make sure it was stored as bytes"
            ):
                raise ValueError(instance.get_bytes("mixed"))
        else:
            assert instance.get_bytes("mixed") == b"is a string"


@contextmanager
def _expect(instance: Cache, **changes: int) -> Iterator[None]:
    before = instance.stats
    yield
    after = instance.stats
    assert after - before == changes, f"Expected following stats changes: {changes}"


@pytest.mark.parametrize(
    "instance",
    ["dynamodb_cache", "in_memory_cache", "local_file_cache", "local_file_cache_no_compression"],
    indirect=True,
)
def test_cache_stats(instance: Cache) -> None:
    stats_fields = {"clears", "deletes", "gets", "hits", "misses", "sets"}
    for field in stats_fields:
        assert getattr(instance.stats, field) == 0
    with _expect(instance):
        instance.hit("does not exist")
    with _expect(instance, gets=1, misses=1):
        instance.get("does not exist")
    with _expect(instance, sets=1):
        instance.set("exists now", "true")
    with _expect(instance, gets=1, hits=1):
        instance.get("exists now")
    with _expect(instance, gets=2, hits=2):
        instance.get("exists now")
        instance.get("exists now")
    with _expect(instance, sets=1, gets=1), instance.open("this is new", mode="str") as file:
        file.write("test")
    with _expect(instance, gets=1, misses=1):
        instance.get_bytes("nonexistent bytes")
    with _expect(instance, sets=1):
        instance.set_bytes("now existent bytes", b"test")
    with _expect(instance, gets=1, hits=1):
        instance.get_bytes("now existent bytes")
    with _expect(instance, sets=1):
        instance.set("another key", "test")
    assert instance.stats.deletes == 0, "no operation up until now should have updated deletes"
    assert instance.stats.clears == 0, "no operation up until now should have updated clears"
    with _expect(instance, deletes=1):
        instance.delete("does not exist")
    with _expect(instance, deletes=1):
        instance.delete("exists now")
    with _expect(instance, clears=1):
        instance.clear()


@pytest.mark.parametrize("local_file_cache_maxsize", [1024], indirect=True)
def test_local_file_cache_cleanup(local_file_cache_maxsize: LocalFileCache) -> None:
    local_file_cache_maxsize.set_bytes("large", os.urandom(4096))
    assert local_file_cache_maxsize.hit("large")
    local_file_cache_maxsize.shrink_to_fit_max_size()
    assert not local_file_cache_maxsize.hit("large")


@pytest.mark.parametrize("local_file_cache_expire", [2], indirect=True)
def test_local_file_cache_expires(local_file_cache_expire: LocalFileCache) -> None:
    assert not local_file_cache_expire.hit("test-expire")
    local_file_cache_expire.set("test-expire", "test")
    assert local_file_cache_expire.get("test-expire") == "test"
    sleep(2)
    assert not local_file_cache_expire.hit("test-expire")


def test_local_file_cache_empty_is_deleted_on_close(local_file_cache_no_compression: LocalFileCache) -> None:
    assert not local_file_cache_no_compression.hit("test-autodelete")
    with local_file_cache_no_compression.open("test-autodelete", "str") as _:
        pass
    assert not local_file_cache_no_compression.hit("test-autodelete")


def test_cache_stats_subtraction() -> None:
    a = CacheStats(hits=69, misses=420, gets=42, deletes=1, clears=2, sets=7)
    b = CacheStats(hits=1, misses=1, gets=2, deletes=2, clears=0, sets=5)
    assert a - b == {"hits": 68, "misses": 419, "gets": 40, "deletes": -1, "sets": 2, "clears": 2}
