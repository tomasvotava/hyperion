from pathlib import Path

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
