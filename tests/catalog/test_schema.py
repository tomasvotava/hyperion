import json
from pathlib import Path

import boto3
import pytest

from hyperion.catalog.schema import LocalSchemaStore, S3SchemaStore, SchemaStore


def test_schema_store_is_parametrized_singleton(tmp_path: Path) -> None:
    nondefault_store = SchemaStore.from_path(tmp_path.as_posix())
    assert nondefault_store is SchemaStore.from_path(tmp_path.as_posix())


def test_schema_store_from_path(tmp_path: Path) -> None:
    filesystem_store = SchemaStore.from_path(tmp_path.as_posix())
    filesystem_store_prefixed = SchemaStore.from_path(f"file://{tmp_path.as_posix()}")
    s3_store = SchemaStore.from_path("s3://this-bucket-does-not-exist/prefix")
    assert isinstance(filesystem_store, LocalSchemaStore)
    assert isinstance(filesystem_store_prefixed, LocalSchemaStore)
    assert filesystem_store.path == filesystem_store_prefixed.path

    assert isinstance(s3_store, S3SchemaStore)
    assert s3_store.bucket == "this-bucket-does-not-exist"
    assert s3_store.prefix == "prefix"


# -----------------------------------------------------------------------------
# Group B3: harden the SchemaStore contract. Pins behaviour that the refactor
# preserves across the move to `ports/schema_registry.py` + `adapters/schema_registry/*`.
# -----------------------------------------------------------------------------


def test_unsupported_scheme_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # The error path inside `_create_new` logs `storage_config.schema_path`, which
    # itself fails if `HYPERION_STORAGE_SCHEMA_PATH` is unset. Provide a value so
    # the log line doesn't shadow the assertion target.
    monkeypatch.setenv("HYPERION_STORAGE_SCHEMA_PATH", "ftp://nope/schema")
    with pytest.raises(ValueError, match="Unsupported schema store scheme"):
        SchemaStore.from_path("ftp://nope/schema")


def test_local_schema_store_missing_path_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    with pytest.raises(FileNotFoundError):
        LocalSchemaStore(missing)


def test_local_schema_store_get_schema_reads_file(tmp_path: Path) -> None:
    asset_type_dir = tmp_path / "data_lake"
    asset_type_dir.mkdir()
    schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
    (asset_type_dir / "my-asset.v1.avro.json").write_text(json.dumps(schema))

    store = LocalSchemaStore(tmp_path)
    assert store.get_schema("my-asset", 1, "data_lake") == schema


def test_local_schema_store_missing_schema_raises(tmp_path: Path) -> None:
    store = LocalSchemaStore(tmp_path)
    with pytest.raises(FileNotFoundError):
        store.get_schema("not-there", 1, "data_lake")


@pytest.mark.usefixtures("_moto_server")
def test_s3_schema_store_get_schema_via_moto() -> None:
    s3 = boto3.client("s3")
    bucket = "schemas-test-bucket"
    s3.create_bucket(Bucket=bucket)
    schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
    key = "data_lake/asset-a.v1.avro.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(schema).encode("utf-8"))

    store = S3SchemaStore(bucket=bucket, prefix="")
    assert store.get_schema("asset-a", 1, "data_lake") == schema


@pytest.mark.usefixtures("_moto_server")
def test_s3_schema_store_lru_cache_avoids_second_call() -> None:
    """The module-level `_get_schema_from_s3` is wrapped in `lru_cache` so repeated
    requests for the same (bucket, key) only hit S3 once. Verify cache is at play
    by counting the number of S3 GETs via a request-counting wrapper around the
    underlying boto3 client.
    """
    from hyperion.adapters.schema_registry import s3 as schema_module

    s3 = boto3.client("s3")
    bucket = "schemas-lru-bucket"
    s3.create_bucket(Bucket=bucket)
    payload = {"type": "record", "name": "Y", "fields": [{"name": "id", "type": "string"}]}
    s3.put_object(Bucket=bucket, Key="data_lake/asset-b.v1.avro.json", Body=json.dumps(payload).encode())

    schema_module._get_schema_from_s3.cache_clear()

    store = S3SchemaStore(bucket=bucket, prefix="")
    call_count = {"n": 0}
    real_download = store.s3_client.download_as_string

    def counting_download(b: str, k: str) -> str:
        call_count["n"] += 1
        return real_download(b, k)

    store.s3_client.download_as_string = counting_download  # type: ignore[assignment]
    first = store.get_schema("asset-b", 1, "data_lake")
    second = store.get_schema("asset-b", 1, "data_lake")
    assert first == second == payload
    # LRU cache served the second request.
    assert call_count["n"] == 1
