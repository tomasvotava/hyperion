from pathlib import Path

from hyperion.catalog.schema import SchemaStore


def test_schema_store_is_parametrized_singleton(tmp_path: Path) -> None:
    nondefault_store = SchemaStore.from_path(tmp_path.as_posix())
    assert nondefault_store is SchemaStore.from_path(tmp_path.as_posix())
