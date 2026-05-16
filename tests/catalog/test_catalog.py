import datetime
import io
import json
import re
import types
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import fastavro
import pytest

from hyperion.adapters.storage.filesystem import FilesystemStorage
from hyperion.adapters.storage.memory import MemoryStorage
from hyperion.adapters.storage.s3 import S3Storage
from hyperion.catalog.catalog import AssetNotFoundError, Catalog, PersistentStore, WritablePersistentStore
from hyperion.catalog.schema import LocalSchemaStore
from hyperion.dateutils import TimeResolution, assure_timezone, quantize_datetime, utcnow
from hyperion.domain.assets import AssetProtocol, DataLakeAsset, FeatureAsset, PersistentStoreAsset
from hyperion.infrastructure.cache import LocalFileCache
from hyperion.infrastructure.message_queue import (
    ArrivalEvent,
    DataLakeArrivalMessage,
    InMemoryQueue,
)
from hyperion.ports.storage import StoragePort

USERS_PARTITION_DATE = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
PLACES_PARTITION_DATES = (
    datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    datetime.datetime(2025, 2, 1, tzinfo=datetime.timezone.utc),
)
SUPERFEATURE_DATE_START = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
SUPERFEATURE_PARTITION_DATES = [
    quantize_datetime(SUPERFEATURE_DATE_START + datetime.timedelta(days=day), "1d") for day in range(0, 7)
]

# Backend matrix the catalog suite runs against (DDD refactor Step 5 / F1). The
# same tests exercise an in-memory store, a local filesystem store, an
# S3 store (moto) and a cache-wrapped store -- proving the restored
# "Catalog on local disk" promise and S3 parity from one test body.
CATALOG_BACKENDS = ["memory", "filesystem", "s3", "cached"]


def _seed_storage(storage: StoragePort, data_dir: Path) -> None:
    """Put the fake data lake + feature assets into ``storage``.

    Storage-agnostic replacement for the old direct-to-S3 seeding: the catalog
    now addresses objects by ``asset.get_path()`` regardless of backend.
    """
    storage.put(
        DataLakeAsset("users", USERS_PARTITION_DATE).get_path(),
        (data_dir / "assets/users.v1.avro").read_bytes(),
    )
    for place_partition_date in PLACES_PARTITION_DATES:
        storage.put(
            DataLakeAsset("places", place_partition_date).get_path(),
            (data_dir / "assets/places.v1.avro").read_bytes(),
        )
    for feature_file in sorted((data_dir / "assets").glob("superfeature.*.v1.avro")):
        partition_date_str = feature_file.name.split(".")[1]
        partition_date = assure_timezone(datetime.datetime.fromisoformat(partition_date_str))
        storage.put(
            FeatureAsset("superfeature", partition_date, "1d").get_path(),
            feature_file.read_bytes(),
        )


def _make_storage(kind: str, tmp_path_factory: pytest.TempPathFactory) -> StoragePort:
    if kind == "memory" or kind == "cached":
        return MemoryStorage()
    if kind == "filesystem":
        return FilesystemStorage(tmp_path_factory.mktemp("fs-catalog"))
    if kind == "s3":
        bucket = f"hyperion-catalog-test-{uuid4().hex}"
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
        return S3Storage(bucket)
    raise ValueError(f"Unknown catalog backend {kind!r}.")


@pytest.fixture(name="test_tmp_dir", scope="session")
def _test_tmp_dir(data_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp_path = tmp_path_factory.mktemp("hyp_catalog")
    (tmp_path / "schemas").mkdir(parents=True)
    (tmp_path / "schemas/custom_schema.json").write_text((data_dir / "assets/custom_schema.json").read_text())
    return tmp_path


@pytest.fixture(name="catalog")
def _catalog(
    request: pytest.FixtureRequest,
    data_dir: Path,
    test_tmp_dir: Path,
    tmp_path_factory: pytest.TempPathFactory,
    _moto_server: None,
) -> Catalog:
    kind = request.param
    storage = _make_storage(kind, tmp_path_factory)
    _seed_storage(storage, data_dir)
    cache = (
        LocalFileCache("test", default_ttl=3600, root_path=tmp_path_factory.mktemp("cache"))
        if kind == "cached"
        else None
    )
    return Catalog(
        storage=storage,
        cache=cache,
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
        queue=InMemoryQueue(),
    )


@pytest.fixture
def queue_aware_catalog(test_tmp_dir: Path) -> Catalog:
    # Queue notification behaviour is storage-independent; a single in-memory
    # backend keeps these tests fast and deterministic.
    return Catalog(
        storage=MemoryStorage(),
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
        queue=InMemoryQueue(),
    )


def _get_test_asset_fields(asset_name: str, data_dir: Path) -> set[str]:
    schema_file = data_dir / f"assets/{asset_name}.v1.avro.json"
    with schema_file.open("r") as file:
        schema = json.load(file)
    return {field["name"] for field in schema["fields"]}


def _get_test_asset_data(asset_name: str, data_dir: Path) -> list[dict[str, Any]]:
    data_file = data_dir / f"assets/{asset_name}.json"
    with data_file.open("r") as file:
        data = json.load(file)
        if not isinstance(data, list):
            raise TypeError(f"Unexpected test data type, expected 'list', got {type(data)!r}.")
        return [
            {
                key: assure_timezone(datetime.datetime.fromisoformat(value)) if key == "date" else value
                for key, value in row.items()
            }
            for row in data
        ]


def _get_test_asset_size(asset_name: str, data_dir: Path) -> int:
    avro_file = data_dir / f"assets/{asset_name}.v1.avro"
    return avro_file.stat().st_size


def _read_avro_via_storage(catalog: Catalog, asset: AssetProtocol) -> tuple[list[Any], dict[str, Any]]:
    """Round-trip helper: pull an avro blob straight out of the catalog's storage and decode it."""
    raw = catalog._resolve_storage(asset).get(asset.get_path())
    reader = fastavro.reader(io.BytesIO(raw))
    records: list[Any] = list(reader)
    metadata: dict[str, Any] = dict(reader.metadata)
    return records, metadata


@pytest.mark.parametrize("catalog", CATALOG_BACKENDS, indirect=True)
class TestCatalog:
    def test_iter_datalake_partitions(self, catalog: Catalog) -> None:
        # iter_datalake_partitions yields in storage-listing order, which is not
        # part of its contract (ordered access is find_latest_datalake_partition).
        assert list(catalog.iter_datalake_partitions("users")) == [DataLakeAsset("users", USERS_PARTITION_DATE)]
        assert sorted(catalog.iter_datalake_partitions("places"), key=lambda asset: asset.date) == [
            DataLakeAsset("places", place_partition_date) for place_partition_date in PLACES_PARTITION_DATES
        ]

    def test_iter_features(self, catalog: Catalog) -> None:
        expected_features = [
            FeatureAsset("superfeature", partition_date, "1d") for partition_date in SUPERFEATURE_PARTITION_DATES
        ]
        features = catalog.iter_feature_store_partitions(
            "superfeature", TimeResolution(1, "d"), SUPERFEATURE_DATE_START, SUPERFEATURE_PARTITION_DATES[-1]
        )
        assert list(features) == expected_features

        features = catalog.iter_feature_store_partitions(
            "superfeature", TimeResolution(1, "d"), SUPERFEATURE_DATE_START, SUPERFEATURE_DATE_START
        )
        assert len(list(features)) == 1

    def test_retrieve_nonexistent(self, catalog: Catalog) -> None:
        foobar = DataLakeAsset("foobar", utcnow())
        data = catalog.retrieve_asset(foobar)
        with pytest.raises(AssetNotFoundError, match=r"Asset 'foobar' not found in the catalog."):
            next(data)

    def test_get_size_nonexistent(self, catalog: Catalog) -> None:
        foobar = DataLakeAsset("foobar", utcnow())
        with pytest.raises(AssetNotFoundError, match=r"Asset 'foobar' not found in the catalog."):
            catalog.get_asset_file_size(foobar)

    def test_get_latest_datalake_partition(self, catalog: Catalog) -> None:
        partition = catalog.find_latest_datalake_partition("places")
        assert partition == DataLakeAsset("places", PLACES_PARTITION_DATES[-1])
        assert partition != DataLakeAsset("places", PLACES_PARTITION_DATES[0]), "Test data inconsistency"

    def test_get_latest_datalake_partition_date_part(self, catalog: Catalog) -> None:
        date_part = PLACES_PARTITION_DATES[0].strftime("%Y-%m-%d")
        partition = catalog.find_latest_datalake_partition("places", date_part)
        assert partition == DataLakeAsset("places", PLACES_PARTITION_DATES[0])
        assert partition != DataLakeAsset("places", PLACES_PARTITION_DATES[-1]), "Test data inconsistency"

    def test_store_asset_with_custom_schema(self, catalog: Catalog) -> None:
        assert isinstance(catalog.schema_store, LocalSchemaStore), "This test only works with LocalSchemaStore"
        schema_path = catalog.schema_store.schemas_path / "custom_schema.json"
        catalog.store_asset(
            DataLakeAsset("nonsensical", utcnow()),
            [{"id": "some id", "custom": True}],
            schema_path=schema_path.as_posix(),
        )

    def test_store_asset_custom_schema_raises(self, catalog: Catalog) -> None:
        assert isinstance(catalog.schema_store, LocalSchemaStore), "This test only works with LocalSchemaStore"
        schema_path = catalog.schema_store.schemas_path / "custom_schema.json"
        with pytest.raises(fastavro.validation.ValidationError, match=r".*Field.Custom.id. is None expected string.*"):
            catalog.store_asset(
                DataLakeAsset("does it even make sense", utcnow()),
                [{"this schema": "is invalid"}],
                schema_path=schema_path.as_posix(),
            )

    @pytest.mark.parametrize(
        "asset",
        [
            DataLakeAsset("users", USERS_PARTITION_DATE),
            *(DataLakeAsset("places", place_partition_date) for place_partition_date in PLACES_PARTITION_DATES),
        ],
    )
    class TestAssetRetrieval:
        def test_retrieve_data_lake_asset_sync(self, catalog: Catalog, asset: DataLakeAsset, data_dir: Path) -> None:
            data = list(catalog.retrieve_asset(asset))
            assert data
            assert isinstance(data[0], dict)
            fields = _get_test_asset_fields(asset.name, data_dir)
            assert set(data[0].keys()) == fields

        def test_asset_data_consistency(self, catalog: Catalog, asset: DataLakeAsset, data_dir: Path) -> None:
            asset_data = catalog.retrieve_asset(asset)
            test_data = _get_test_asset_data(asset.name, data_dir)
            assert list(asset_data) == test_data

        def test_get_asset_size(self, catalog: Catalog, asset: DataLakeAsset, data_dir: Path) -> None:
            expected_size = _get_test_asset_size(asset.name, data_dir)
            assert catalog.get_asset_file_size(asset) == expected_size


# -----------------------------------------------------------------------------
# Constructor-inversion contract (DDD refactor F1 / Step 5).
# -----------------------------------------------------------------------------


class TestStorageRouting:
    """`Catalog` accepts a single port (one store for everything) or a
    per-asset-type mapping (how `from_config()` keeps today's 3-bucket layout).
    """

    def test_single_port_serves_every_asset_type(self, test_tmp_dir: Path) -> None:
        storage = MemoryStorage()
        catalog = Catalog(storage=storage, schema_store=LocalSchemaStore(test_tmp_dir / "schemas"))
        assert catalog._resolve_storage(DataLakeAsset) is storage
        assert catalog._resolve_storage(FeatureAsset) is storage
        assert catalog._resolve_storage(DataLakeAsset("x", utcnow())) is storage

    def test_mapping_routes_per_asset_type(self, test_tmp_dir: Path) -> None:
        data_lake = MemoryStorage()
        feature = MemoryStorage()
        persistent = MemoryStorage()
        catalog = Catalog(
            storage={"data_lake": data_lake, "feature": feature, "persistent_store": persistent},
            schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
        )

        schema = {"type": "record", "name": "R", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "routing.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        dl_asset = DataLakeAsset("routed", utcnow())
        catalog.store_asset(dl_asset, [{"id": "a"}], notify=False, schema_path=schema_path.as_posix())

        # Landed only in the data-lake backend, nowhere else.
        assert dl_asset.get_path() in data_lake._store
        assert feature._store == {}
        assert persistent._store == {}
        assert list(catalog.retrieve_asset(dl_asset)) == [{"id": "a"}]

    def test_incomplete_mapping_is_rejected(self, test_tmp_dir: Path) -> None:
        with pytest.raises(ValueError, match=r"missing adapters for asset types"):
            Catalog(
                storage={"data_lake": MemoryStorage()},
                schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
            )

    def test_old_bucket_kwargs_are_a_hard_break(self, test_tmp_dir: Path) -> None:
        # The pre-1.0 bucket/prefix constructor is gone; only from_config() (or an
        # explicit StoragePort) is supported now.
        with pytest.raises(TypeError):
            Catalog(  # type: ignore[call-arg]
                data_lake_bucket="dl",
                feature_store_bucket="fs",
                persistent_store_bucket="ps",
            )


class TestLocalDiskPromise:
    """DoD: a `[catalog]`-only consumer can run `Catalog` against local disk
    (no `[aws]`).
    """

    def test_filesystem_round_trip(self, tmp_path: Path) -> None:
        # LocalSchemaStore resolves schemas at <root>/<asset_type>/<name>.v<n>.avro.json.
        schema_dir = tmp_path / "schemas" / "data_lake"
        schema_dir.mkdir(parents=True)
        schema = {"type": "record", "name": "Local", "fields": [{"name": "id", "type": "string"}]}
        (schema_dir / "diskasset.v1.avro.json").write_text(json.dumps(schema))

        catalog = Catalog(
            storage=FilesystemStorage(tmp_path / "data"),
            schema_store=LocalSchemaStore(tmp_path / "schemas"),
        )
        asset = DataLakeAsset("diskasset", datetime.datetime(2025, 3, 1, tzinfo=datetime.timezone.utc))
        records = [{"id": "one"}, {"id": "two"}]
        catalog.store_asset(asset, records, notify=False)

        assert (tmp_path / "data" / asset.get_path()).is_file()
        assert list(catalog.retrieve_asset(asset)) == records
        assert catalog.find_latest_datalake_partition("diskasset") == asset


@pytest.mark.usefixtures("_moto_server")
def test_from_config_uses_correct_per_store_prefixes(monkeypatch: pytest.MonkeyPatch) -> None:
    """`from_config()` must wire the feature store with the *feature* prefix.

    Pins the Step 5 bug-fix: the pre-refactor `from_config()` fed the data-lake
    prefix into the feature store.
    """
    fake_config = types.SimpleNamespace(
        data_lake_bucket="dl-bucket",
        feature_store_bucket="fs-bucket",
        persistent_store_bucket="ps-bucket",
        data_lake_prefix="dl",
        feature_store_prefix="fs",
        persistent_store_prefix="ps",
    )
    monkeypatch.setattr("hyperion.catalog.catalog.storage_config", fake_config)
    # from_config() builds a default SchemaStore from env; stub it out so this
    # test stays focused on storage wiring.
    monkeypatch.setattr("hyperion.catalog.catalog.SchemaStore", types.SimpleNamespace(from_config=lambda: object()))

    catalog = Catalog.from_config()

    feature_storage = catalog._resolve_storage(FeatureAsset)
    assert isinstance(feature_storage, S3Storage)
    assert feature_storage._bucket == "fs-bucket"
    assert feature_storage._prefix == "fs/"  # not "dl/" -- the fixed bug
    data_lake_storage = catalog._resolve_storage(DataLakeAsset)
    assert isinstance(data_lake_storage, S3Storage)
    assert data_lake_storage._prefix == "dl/"


def test_catalog_module_has_no_direct_boto3_or_fastavro_imports() -> None:
    """S5 deliverable: `catalog.py` itself no longer references boto3/fastavro.

    (A transitive boto3 still arrives via `infrastructure.message_queue`; that
    module's concretes relocate in S6, so a `sys.modules` check is premature.)
    """
    source = Path(Catalog.__module__.replace(".", "/") + ".py")
    if not source.exists():  # pragma: no cover - import layout fallback
        import hyperion.catalog.catalog as catalog_module

        source = Path(catalog_module.__file__)
    text = source.read_text()
    offenders = re.findall(r"^(?:import|from)\s+(?:boto3|botocore|fastavro)\b.*$", text, flags=re.MULTILINE)
    offenders += re.findall(r"^.*\bS3Client\b.*$", text, flags=re.MULTILINE)
    assert offenders == [], f"catalog.py must not import boto3/fastavro/S3Client directly: {offenders}"


# -----------------------------------------------------------------------------
# Contract-hardening tests (avro byte-contract, queue notifications, key parsing).
# -----------------------------------------------------------------------------


class TestAvroRoundtrip:
    """Lock the avro encode/decode contract so the extracted `AvroSerializer`
    keeps byte-level parity with the pre-refactor inline implementation.
    """

    def test_primitive_types_roundtrip(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {
            "type": "record",
            "name": "Primitives",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "count", "type": "long"},
                {"name": "weight", "type": "double"},
                {"name": "flag", "type": "boolean"},
                {"name": "maybe", "type": ["null", "string"]},
            ],
        }
        schema_path = test_tmp_dir / "schemas" / "primitives.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        data = [
            {"id": "a", "count": 1, "weight": 1.5, "flag": True, "maybe": "yes"},
            {"id": "b", "count": -2, "weight": 0.0, "flag": False, "maybe": None},
        ]
        asset = DataLakeAsset("primitives", utcnow())
        queue_aware_catalog.store_asset(asset, data, notify=False, schema_path=schema_path.as_posix())

        retrieved = list(queue_aware_catalog.retrieve_asset(asset))
        assert retrieved == data

    def test_array_and_record_types_roundtrip(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {
            "type": "record",
            "name": "Composite",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                    "name": "nested",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                        ],
                    },
                },
            ],
        }
        schema_path = test_tmp_dir / "schemas" / "composite.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        data = [{"tags": ["a", "b"], "nested": {"x": 1, "y": 2}}]
        asset = DataLakeAsset("composite", utcnow())
        queue_aware_catalog.store_asset(asset, data, notify=False, schema_path=schema_path.as_posix())

        retrieved = list(queue_aware_catalog.retrieve_asset(asset))
        assert retrieved == data

    def test_metadata_written_to_avro(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {
            "type": "record",
            "name": "MetadataCheck",
            "fields": [{"name": "id", "type": "string"}],
        }
        schema_path = test_tmp_dir / "schemas" / "metadata-check.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        date = datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)
        asset = DataLakeAsset("metadata-check", date)
        queue_aware_catalog.store_asset(asset, [{"id": "x"}], notify=False, schema_path=schema_path.as_posix())

        _, metadata = _read_avro_via_storage(queue_aware_catalog, asset)
        # fastavro returns metadata keys/values as bytes.
        decoded = {
            k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
            for k, v in metadata.items()
        }
        assert decoded.get("name") == "metadata-check"
        assert decoded.get("schema_version") == "1"
        assert decoded.get("date") == date.isoformat()


class TestQueueNotification:
    def test_store_data_lake_asset_sends_arrival_message(
        self, queue_aware_catalog: Catalog, test_tmp_dir: Path
    ) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "notify-check.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        asset = DataLakeAsset("notify-check", utcnow())
        queue_aware_catalog.store_asset(asset, [{"id": "x"}], notify=True, schema_path=schema_path.as_posix())

        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        assert len(queue._messages) == 1
        message = queue._messages[0]
        assert isinstance(message, DataLakeArrivalMessage)
        assert message.asset == asset
        assert message.event == ArrivalEvent.ARRIVED
        assert message.schema_path == schema_path.as_posix()

    def test_notify_false_skips_message(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "notify-false.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        asset = DataLakeAsset("notify-false", utcnow())
        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        before = len(queue._messages)
        queue_aware_catalog.store_asset(asset, [{"id": "x"}], notify=False, schema_path=schema_path.as_posix())
        assert len(queue._messages) == before

    def test_non_datalake_asset_does_not_notify(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "feature-no-notify.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        feature_asset = FeatureAsset("feature-no-notify", utcnow(), "1d")
        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        before = len(queue._messages)
        queue_aware_catalog.store_asset(feature_asset, [{"id": "x"}], notify=True, schema_path=schema_path.as_posix())
        assert len(queue._messages) == before


@pytest.mark.parametrize("catalog", CATALOG_BACKENDS, indirect=True)
class TestPartitionIterationRegex:
    """Pins the key-parsing contract of `iter_datalake_partitions`. The adapter
    now owns key iteration; the regex must still skip non-conforming keys.
    """

    def test_multiple_versions_for_same_date(self, catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "multiver.v1.avro.json"
        schema_path.write_text(json.dumps(schema))
        date = datetime.datetime(2025, 6, 1, tzinfo=datetime.timezone.utc)
        catalog.store_asset(
            DataLakeAsset("multiver", date, schema_version=1),
            [{"id": "v1"}],
            notify=False,
            schema_path=schema_path.as_posix(),
        )
        catalog.store_asset(
            DataLakeAsset("multiver", date, schema_version=2),
            [{"id": "v2"}],
            notify=False,
            schema_path=schema_path.as_posix(),
        )

        partitions = list(catalog.iter_datalake_partitions("multiver"))
        versions = sorted(p.schema_version for p in partitions)
        assert versions == [1, 2]

    def test_skips_keys_with_invalid_filename(self, catalog: Catalog) -> None:
        catalog._resolve_storage(DataLakeAsset).put(
            "junk-asset/date=2025-07-01T00:00:00+00:00/not-an-avro-file.txt", b"junk"
        )
        partitions = list(catalog.iter_datalake_partitions("junk-asset"))
        assert partitions == []

    def test_skips_keys_with_wrong_structure(self, catalog: Catalog) -> None:
        catalog._resolve_storage(DataLakeAsset).put("bad-shape-asset/v1.avro", b"junk")
        partitions = list(catalog.iter_datalake_partitions("bad-shape-asset"))
        assert partitions == []


@pytest.mark.parametrize("catalog", CATALOG_BACKENDS, indirect=True)
class TestFeatureStorePartitionQuantization:
    def test_skips_missing_partitions(self, catalog: Catalog) -> None:
        date_from = SUPERFEATURE_DATE_START - datetime.timedelta(days=14)
        date_to = SUPERFEATURE_PARTITION_DATES[-1] + datetime.timedelta(days=14)
        partitions = list(
            catalog.iter_feature_store_partitions("superfeature", TimeResolution(1, "d"), date_from, date_to)
        )
        assert {p.partition_date for p in partitions} == set(SUPERFEATURE_PARTITION_DATES)


# -----------------------------------------------------------------------------
# Storage-port rewiring of the previously S3Client-only / untested paths
# (persistent store, async store, repartitioner). Now trivially testable
# against MemoryStorage.
# -----------------------------------------------------------------------------

_PSTORE_SCHEMA = {"type": "record", "name": "PStore", "fields": [{"name": "id", "type": "string"}]}
_TS_SCHEMA = {
    "type": "record",
    "name": "Repart",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    ],
}


class TestPersistentStore:
    @pytest.fixture(autouse=True)
    def _isolate_singletons(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # PersistentStore caches instances on a shared class-level dict.
        PersistentStore._instances.clear()
        # WritablePersistentStore.store() builds its schema store from env.
        monkeypatch.setattr(
            "hyperion.catalog.catalog.SchemaStore",
            types.SimpleNamespace(
                from_config=lambda: types.SimpleNamespace(get_asset_schema=lambda asset: _PSTORE_SCHEMA)
            ),
        )

    def test_write_then_retrieve_round_trip(self) -> None:
        storage = MemoryStorage()
        asset = PersistentStoreAsset("pstore")
        WritablePersistentStore(asset, storage).store([{"id": "a"}, {"id": "b"}])

        # Decode straight from storage to confirm the bytes round-trip.
        raw = storage.get(asset.get_path())
        assert list(fastavro.reader(io.BytesIO(raw))) == [{"id": "a"}, {"id": "b"}]

        PersistentStore._instances.clear()
        store = PersistentStore(asset, storage)
        store.retrieve()
        assert store._local_path is not None
        local_path = store._local_path
        assert list(fastavro.reader(local_path.open("rb"))) == [{"id": "a"}, {"id": "b"}]

        # Second retrieve with an unchanged etag must short-circuit (same file).
        store.retrieve()
        assert store._local_path == local_path

        store.cleanup()
        assert store._local_path is None
        assert not local_path.exists()

    def test_retrieve_missing_raises_asset_not_found(self) -> None:
        with pytest.raises(AssetNotFoundError, match=r"Asset 'ghost' not found"):
            PersistentStore(PersistentStoreAsset("ghost"), MemoryStorage()).retrieve()


class TestStoreAssetAsync:
    async def test_store_asset_async_round_trip(self, test_tmp_dir: Path) -> None:
        schema = {"type": "record", "name": "Async", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "async-asset.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        catalog = Catalog(
            storage=MemoryStorage(),
            schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
            queue=InMemoryQueue(),
        )
        asset = DataLakeAsset("async-asset", utcnow())
        data = [{"id": "a"}, {"id": "b"}]
        await catalog.store_asset_async(asset, data, notify=True, schema_path=schema_path.as_posix())

        assert list(catalog.retrieve_asset(asset)) == data
        queue = catalog.queue
        assert isinstance(queue, InMemoryQueue)
        assert len(queue._messages) == 1


class TestRepartition:
    async def test_repartition_data_lake_asset_by_day(self, tmp_path: Path) -> None:
        schema_dir = tmp_path / "schemas" / "data_lake"
        schema_dir.mkdir(parents=True)
        (schema_dir / "repart.v1.avro.json").write_text(json.dumps(_TS_SCHEMA))

        catalog = Catalog(
            storage=MemoryStorage(),
            schema_store=LocalSchemaStore(tmp_path / "schemas"),
            queue=InMemoryQueue(),
        )
        day_one = datetime.datetime(2025, 1, 1, 8, tzinfo=datetime.timezone.utc)
        day_two = datetime.datetime(2025, 1, 2, 9, tzinfo=datetime.timezone.utc)
        records = [
            {"id": "a", "timestamp": day_one},
            {"id": "b", "timestamp": day_one.replace(hour=20)},
            {"id": "c", "timestamp": day_two},
        ]
        source_asset = DataLakeAsset("repart", day_one)

        await catalog.repartition(source_asset, "d", date_attribute="timestamp", data=records)

        partitions = sorted(catalog.iter_datalake_partitions("repart"), key=lambda asset: asset.date)
        assert [p.date for p in partitions] == [
            datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ]
        first_day = list(catalog.retrieve_asset(partitions[0]))
        assert {row["id"] for row in first_day} == {"a", "b"}
        second_day = list(catalog.retrieve_asset(partitions[1]))
        assert {row["id"] for row in second_day} == {"c"}
