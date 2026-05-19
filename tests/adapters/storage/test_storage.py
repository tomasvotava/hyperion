"""Cross-backend conformance tests for the StoragePort adapters.

Mirrors the parametrize-over-backends pattern of
``tests/infrastructure/test_cache.py`` and the moto bucket fixture of
``tests/infrastructure/aws/test_s3.py``. Every adapter starts each test empty,
so the same suite runs identically against memory / filesystem / s3.
"""

import hashlib
import uuid
from io import BytesIO
from pathlib import Path

import boto3
import pytest

from hyperion.adapters.storage.filesystem import FilesystemStorage
from hyperion.adapters.storage.memory import MemoryStorage
from hyperion.adapters.storage.s3 import S3Storage
from hyperion.ports.storage import ObjectNotFoundError, StoragePort

_LARGE_PAYLOAD = b"hyperion-storage-stream-test\n" * 200_000  # ~5.6 MiB


@pytest.fixture
def memory_storage() -> MemoryStorage:
    return MemoryStorage()


@pytest.fixture
def filesystem_storage(tmp_path: Path) -> FilesystemStorage:
    return FilesystemStorage(tmp_path)


@pytest.fixture
def s3_storage(_moto_server: None) -> S3Storage:
    bucket = f"hyperion-storage-test-{uuid.uuid4().hex}"
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
    return S3Storage(bucket)


@pytest.fixture
def instance(
    memory_storage: MemoryStorage,
    filesystem_storage: FilesystemStorage,
    s3_storage: S3Storage,
    request: pytest.FixtureRequest,
) -> StoragePort:
    adapters: dict[str, StoragePort] = {
        "memory_storage": memory_storage,
        "filesystem_storage": filesystem_storage,
        "s3_storage": s3_storage,
    }
    return adapters[request.param]


@pytest.mark.parametrize("instance", ["memory_storage", "filesystem_storage", "s3_storage"], indirect=True)
class TestStoragePort:
    def test_satisfies_protocol(self, instance: StoragePort) -> None:
        assert isinstance(instance, StoragePort)

    def test_put_get_bytes(self, instance: StoragePort) -> None:
        instance.put("dir/obj.bin", b"hello")
        assert instance.get("dir/obj.bin") == b"hello"

    def test_put_get_filelike(self, instance: StoragePort) -> None:
        instance.put("obj.bin", BytesIO(b"streamed"))
        assert instance.get("obj.bin") == b"streamed"

    def test_put_overwrites(self, instance: StoragePort) -> None:
        instance.put("obj.bin", b"first")
        instance.put("obj.bin", b"second")
        assert instance.get("obj.bin") == b"second"

    async def test_put_async(self, instance: StoragePort) -> None:
        await instance.put_async("async/obj.bin", b"async-bytes")
        assert instance.get("async/obj.bin") == b"async-bytes"

    def test_open_streams(self, instance: StoragePort) -> None:
        instance.put("obj.bin", b"streaming read")
        with instance.open("obj.bin") as handle:
            assert handle.read() == b"streaming read"

    def test_iter_keys_filters_by_prefix(self, instance: StoragePort) -> None:
        instance.put("a/1.bin", b"x")
        instance.put("a/2.bin", b"x")
        instance.put("b/3.bin", b"x")
        assert sorted(instance.iter_keys("a/")) == ["a/1.bin", "a/2.bin"]
        assert sorted(instance.iter_keys("")) == ["a/1.bin", "a/2.bin", "b/3.bin"]

    def test_exists(self, instance: StoragePort) -> None:
        assert not instance.exists("obj.bin")
        instance.put("obj.bin", b"x")
        assert instance.exists("obj.bin")

    def test_delete_is_idempotent(self, instance: StoragePort) -> None:
        instance.delete("missing.bin")  # no error
        instance.put("obj.bin", b"x")
        instance.delete("obj.bin")
        assert not instance.exists("obj.bin")
        instance.delete("obj.bin")  # no error second time

    def test_get_attributes(self, instance: StoragePort) -> None:
        instance.put("obj.bin", b"sized payload")
        attrs = instance.get_attributes("obj.bin")
        assert attrs.size == len(b"sized payload")
        assert attrs.etag
        assert attrs.last_modified.tzinfo is not None

    def test_get_missing_raises(self, instance: StoragePort) -> None:
        with pytest.raises(ObjectNotFoundError):
            instance.get("nope.bin")

    def test_open_missing_raises(self, instance: StoragePort) -> None:
        with pytest.raises(ObjectNotFoundError), instance.open("nope.bin"):
            pass

    def test_get_attributes_missing_raises(self, instance: StoragePort) -> None:
        with pytest.raises(ObjectNotFoundError):
            instance.get_attributes("nope.bin")


def test_iter_keys_returns_empty_for_unknown_prefix(memory_storage: MemoryStorage) -> None:
    assert list(memory_storage.iter_keys("anything/")) == []


def test_filesystem_rejects_key_escaping_root(tmp_path: Path) -> None:
    storage = FilesystemStorage(tmp_path)
    with pytest.raises(ValueError, match="escapes the storage root"):
        storage.put("../escape.bin", b"x")


@pytest.mark.usefixtures("_moto_server")
def test_s3_prefix_is_transparent_to_callers() -> None:
    bucket = f"hyperion-storage-prefixed-{uuid.uuid4().hex}"
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
    storage = S3Storage(bucket, prefix="tenant-a/")
    storage.put("data/obj.bin", b"payload")
    assert storage.get("data/obj.bin") == b"payload"
    assert list(storage.iter_keys("data/")) == ["data/obj.bin"]
    raw_keys = [obj["Key"] for obj in boto3.client("s3").list_objects_v2(Bucket=bucket)["Contents"]]
    assert raw_keys == ["tenant-a/data/obj.bin"]


@pytest.mark.usefixtures("_moto_server")
async def test_s3_put_async_does_not_close_caller_stream() -> None:
    bucket = f"hyperion-storage-async-{uuid.uuid4().hex}"
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
    storage = S3Storage(bucket)

    buf = BytesIO(b"caller-owned-stream")
    await storage.put_async("stream/obj.bin", buf)
    assert not buf.closed  # issue #148: caller-owned stream must stay open
    assert storage.get("stream/obj.bin") == b"caller-owned-stream"

    await storage.put_async("bytes/obj.bin", b"async-bytes")
    assert storage.get("bytes/obj.bin") == b"async-bytes"


def test_filesystem_put_streams_large_filelike(filesystem_storage: FilesystemStorage) -> None:
    filesystem_storage.put("big/obj.bin", BytesIO(_LARGE_PAYLOAD))
    assert filesystem_storage.get("big/obj.bin") == _LARGE_PAYLOAD


def test_filesystem_get_attributes_etag_correct_for_large_payload(
    filesystem_storage: FilesystemStorage,
) -> None:
    filesystem_storage.put("big/obj.bin", BytesIO(_LARGE_PAYLOAD))
    attrs = filesystem_storage.get_attributes("big/obj.bin")
    expected_etag = hashlib.md5(_LARGE_PAYLOAD, usedforsecurity=False).hexdigest()
    assert attrs.etag == expected_etag
    assert attrs.size == len(_LARGE_PAYLOAD)
