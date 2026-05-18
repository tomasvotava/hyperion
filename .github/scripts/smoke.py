"""Packaging smoke checks for the S10 extras model (epic #137 DoD).

Run inside a *fresh venv* that has only the relevant install:

    python .github/scripts/smoke.py lite        # bare `pip install hyperion-sdk`
    python .github/scripts/smoke.py aws          # hyperion-sdk[aws]
    python .github/scripts/smoke.py data         # hyperion-sdk[data]
    python .github/scripts/smoke.py catalog      # hyperion-sdk[catalog]  (NO [aws])
    python .github/scripts/smoke.py geo          # hyperion-sdk[geo]      (NO [aws])
    python .github/scripts/smoke.py snappy       # hyperion-sdk[snappy]

Exits non-zero with a clear message on the first failed expectation.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
import tempfile
from pathlib import Path

# Import names (not distribution names) of every extras-gated heavy dependency.
HEAVY = ["boto3", "aioboto3", "polars", "pandera", "numpy", "fastavro", "googlemaps", "snappy"]

# The lite-core import line from the epic #137 / plan Definition of Done.
LITE_IMPORTS = (
    "hyperion.log",
    "hyperion.dateutils",
    "hyperion.config",
    "hyperion.asyncutils",
    "hyperion.domain.assets",
    "hyperion.domain.geo",
    "hyperion.ports.storage",
    "hyperion.ports.cache",
)


def _present(mod: str) -> bool:
    try:
        return importlib.util.find_spec(mod) is not None
    except ModuleNotFoundError:
        # A parent package itself missing -> the leaf is absent.
        return False


def _fail(mode: str, msg: str) -> None:
    sys.exit(f"FAIL[{mode}]: {msg}")


def _require(mode: str, *mods: str) -> None:
    missing = [m for m in mods if not _present(m)]
    if missing:
        _fail(mode, f"expected importable but missing: {missing}")


def _forbid(mode: str, *mods: str) -> None:
    present = [m for m in mods if _present(m)]
    if present:
        _fail(mode, f"must be absent for this install but present: {present}")


def smoke_lite() -> None:
    for mod in LITE_IMPORTS:
        __import__(mod)
    _forbid("lite", *HEAVY)


def smoke_aws() -> None:
    import hyperion.adapters.cache.dynamodb  # noqa: F401
    import hyperion.adapters.storage.s3  # noqa: F401

    _require("aws", "boto3", "aioboto3")


def smoke_data() -> None:
    import hyperion.data.asset_schemas  # noqa: F401
    import hyperion.data.typeutils  # noqa: F401

    _require("data", "polars", "pandera", "numpy")


def smoke_catalog() -> None:
    # [catalog] WITHOUT [aws]: a local-disk Catalog round-trip must work and
    # boto3 must not be present. Mirrors tests/catalog/test_catalog.py
    # ::TestLocalDiskPromise.test_filesystem_round_trip.
    _forbid("catalog", "boto3", "aioboto3")
    _require("catalog", "fastavro")

    from hyperion.adapters.schema_registry.local import LocalSchemaStore
    from hyperion.adapters.storage.filesystem import FilesystemStorage
    from hyperion.catalog.catalog import Catalog
    from hyperion.domain.assets import DataLakeAsset

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        schema_dir = root / "schemas" / "data_lake"
        schema_dir.mkdir(parents=True)
        (schema_dir / "diskasset.v1.avro.json").write_text(
            '{"type": "record", "name": "Local", "fields": [{"name": "id", "type": "string"}]}'
        )
        catalog = Catalog(
            storage=FilesystemStorage(root / "data"),
            schema_store=LocalSchemaStore(root / "schemas"),
        )
        asset = DataLakeAsset("diskasset", datetime.datetime(2025, 3, 1, tzinfo=datetime.UTC))
        records = [{"id": "one"}, {"id": "two"}]
        catalog.store_asset(asset, records, notify=False)
        if list(catalog.retrieve_asset(asset)) != records:
            _fail("catalog", "local-disk Catalog round-trip returned unexpected records")


def smoke_geo() -> None:
    # [geo] WITHOUT [aws]: GoogleMaps constructs with an injected KeyValueStore
    # and pulls no boto3.
    _forbid("geo", "boto3", "aioboto3")
    _require("geo", "googlemaps")

    from hyperion.adapters.geocoder.google import GoogleMaps
    from hyperion.adapters.keyval.memory import InMemoryStore

    # googlemaps.Client validates key *format* offline (must start with "AIza");
    # no network call at construction. This proves the [geo] wiring, not auth.
    GoogleMaps(keyval=InMemoryStore(), api_key="AIza-hyperion-smoke-test-key-0000000000")


def smoke_snappy() -> None:
    import hyperion.adapters.cache.filesystem  # noqa: F401

    _require("snappy", "snappy")


_MODES = {
    "lite": smoke_lite,
    "aws": smoke_aws,
    "data": smoke_data,
    "catalog": smoke_catalog,
    "geo": smoke_geo,
    "snappy": smoke_snappy,
}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in _MODES:
        sys.exit(f"usage: smoke.py <{'|'.join(_MODES)}>")
    mode = sys.argv[1]
    _MODES[mode]()
    print(f"OK[{mode}]: packaging smoke passed")


if __name__ == "__main__":
    main()
