import datetime
import json
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import fastavro
import pytest

from hyperion.catalog.catalog import AssetNotFoundError, Catalog
from hyperion.catalog.schema import LocalSchemaStore
from hyperion.dateutils import TimeResolution, assure_timezone, quantize_datetime, utcnow
from hyperion.entities.catalog import AssetProtocol, AssetType, DataLakeAsset, FeatureAsset, PersistentStoreAsset
from hyperion.infrastructure.cache import LocalFileCache
from hyperion.infrastructure.message_queue import (
    ArrivalEvent,
    DataLakeArrivalMessage,
    InMemoryQueue,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

DATA_LAKE_BUCKET = "test-data-lake"
FEATURE_STORE_BUCKET = "test-feature-store"
PERSISTENT_STORE_BUCKET = "test-persistent-store"
PREFIXED_BUCKET = "test-prefixed-store"
DATA_LAKE_PREFIX = "data-lake/"
FEATURE_STORE_PREFIX = "feature-store/"
PERSISTENT_STORE_PREFIX = "persistent-store/"

USERS_PARTITION_DATE = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
PLACES_PARTITION_DATES = (
    datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    datetime.datetime(2025, 2, 1, tzinfo=datetime.timezone.utc),
)
SUPERFEATURE_DATE_START = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
SUPERFEATURE_PARTITION_DATES = [
    quantize_datetime(SUPERFEATURE_DATE_START + datetime.timedelta(days=day), "1d") for day in range(0, 7)
]


@pytest.fixture(name="s3client", scope="module")
def _s3client(_moto_server: None) -> "S3Client":
    s3 = boto3.client("s3")
    buckets = (DATA_LAKE_BUCKET, FEATURE_STORE_BUCKET, PERSISTENT_STORE_BUCKET, PREFIXED_BUCKET)
    for bucket in buckets:
        s3.create_bucket(Bucket=bucket)
    return s3


def _create_fake_asset(
    asset_name: str, s3client: "S3Client", partition_date: datetime.datetime, asset_type: AssetType, avro_file: Path
) -> None:
    bucket: str
    prefix: str
    match asset_type:
        case "data_lake":
            bucket = DATA_LAKE_BUCKET
            prefix = DATA_LAKE_PREFIX
        case "feature":
            bucket = FEATURE_STORE_BUCKET
            prefix = FEATURE_STORE_PREFIX
        case "persistent_store":
            raise NotImplementedError("Testing persistent store assets is not implemented yet.")
            # bucket = PERSISTENT_STORE_BUCKET
            # prefix = PERSISTENT_STORE_PREFIX
        case _:
            raise ValueError(f"Unsupported asset type {asset_type!r}.")
    partition_name = "partition_date" if asset_type == "feature" else "date"
    path = f"{asset_name}/{partition_name}={partition_date.isoformat()}/v1.avro"
    avro_file = avro_file.resolve()
    s3client.upload_file(Filename=avro_file.as_posix(), Bucket=bucket, Key=path)
    s3client.upload_file(Filename=avro_file.as_posix(), Bucket=PREFIXED_BUCKET, Key=f"{prefix}{path}")


def _create_fake_data_lake(data_dir: Path, s3client: "S3Client") -> None:
    _create_fake_asset("users", s3client, USERS_PARTITION_DATE, "data_lake", data_dir / "assets/users.v1.avro")
    for place_partition_date in PLACES_PARTITION_DATES:
        _create_fake_asset("places", s3client, place_partition_date, "data_lake", data_dir / "assets/places.v1.avro")


def _create_fake_feature_store(data_dir: Path, s3client: "S3Client") -> None:
    for feature_file in (data_dir / "assets").glob("superfeature.*.v1.avro"):
        partition_date_str = feature_file.name.split(".")[1]
        partition_date = assure_timezone(datetime.datetime.fromisoformat(partition_date_str))
        _create_fake_asset("superfeature.1d", s3client, partition_date, "feature", feature_file)


@pytest.fixture(scope="module")
def _test_data(data_dir: Path, s3client: "S3Client") -> None:
    _create_fake_data_lake(data_dir, s3client)
    _create_fake_feature_store(data_dir, s3client)


@pytest.fixture(name="test_tmp_dir", scope="session")
def _test_tmp_dir(data_dir: Path) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="tmphyp_") as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "schemas").mkdir(parents=True)
        (tmp_path / "cache").mkdir(parents=True)
        (tmp_path / "schemas/custom_schema.json").write_text((data_dir / "assets/custom_schema.json").read_text())
        yield tmp_path


@pytest.fixture(name="default_catalog", scope="session")
def _default_catalog(test_tmp_dir: Path) -> Catalog:
    return Catalog(
        data_lake_bucket=DATA_LAKE_BUCKET,
        feature_store_bucket=FEATURE_STORE_BUCKET,
        persistent_store_bucket=PERSISTENT_STORE_BUCKET,
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
    )


@pytest.fixture(name="prefixed_catalog", scope="session")
def _prefixed_catalog(test_tmp_dir: Path) -> Catalog:
    return Catalog(
        data_lake_bucket=PREFIXED_BUCKET,
        feature_store_bucket=PREFIXED_BUCKET,
        persistent_store_bucket=PREFIXED_BUCKET,
        data_lake_prefix=DATA_LAKE_PREFIX,
        feature_store_prefix=FEATURE_STORE_PREFIX,
        persistent_store_prefix=PERSISTENT_STORE_PREFIX,
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
    )


@pytest.fixture(name="cached_catalog", scope="session")
def _cached_catalog(test_tmp_dir: Path) -> Catalog:
    return Catalog(
        data_lake_bucket=DATA_LAKE_BUCKET,
        feature_store_bucket=FEATURE_STORE_BUCKET,
        persistent_store_bucket=PERSISTENT_STORE_BUCKET,
        cache=LocalFileCache("test", default_ttl=3600, root_path=test_tmp_dir / "cache"),
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
    )


@pytest.fixture(name="catalog")
def _catalog(
    default_catalog: Catalog, prefixed_catalog: Catalog, cached_catalog: Catalog, request: pytest.FixtureRequest
) -> Catalog:
    return {"default_catalog": default_catalog, "prefixed_catalog": prefixed_catalog, "cached_catalog": cached_catalog}[
        request.param
    ]


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


@pytest.mark.parametrize(
    ("asset", "expected_bucket", "expected_prefix"),
    [
        (DataLakeAsset("test", utcnow()), DATA_LAKE_BUCKET, DATA_LAKE_PREFIX),
        (FeatureAsset("test", utcnow(), "1d"), FEATURE_STORE_BUCKET, FEATURE_STORE_PREFIX),
        (PersistentStoreAsset("test"), PERSISTENT_STORE_BUCKET, PERSISTENT_STORE_PREFIX),
    ],
)
def test_store_config(
    default_catalog: Catalog,
    prefixed_catalog: Catalog,
    asset: AssetProtocol,
    expected_bucket: str,
    expected_prefix: str,
) -> None:
    store_config = default_catalog.get_store_config(asset)
    prefixed_config = prefixed_catalog.get_store_config(asset)

    assert store_config["bucket"] == expected_bucket
    assert store_config["prefix"] == ""

    assert prefixed_config["bucket"] == PREFIXED_BUCKET
    assert prefixed_config["prefix"] == expected_prefix

    assert f"{asset.name}" in asset.get_path()
    assert "v1.avro" in asset.get_path()
    assert asset.get_path(expected_prefix).startswith(f"{expected_prefix}{asset.name}")

    if isinstance(asset, FeatureAsset):
        assert str(asset.resolution) in asset.get_path()


@pytest.mark.parametrize("catalog", ["default_catalog", "prefixed_catalog", "cached_catalog"], indirect=True)
@pytest.mark.usefixtures("_test_data")
class TestCatalog:
    def test_iter_datalake_partitions(self, catalog: Catalog) -> None:
        assert list(catalog.iter_datalake_partitions("users")) == [DataLakeAsset("users", USERS_PARTITION_DATE)]
        assert list(catalog.iter_datalake_partitions("places")) == [
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
# Contract-hardening tests (Group B1 of the DDD refactor prep plan).
#
# These pin behaviours that today only exist implicitly. After the refactor adds
# `StoragePort`, the same tests will be parametrized over `S3Storage(moto)` and
# `FilesystemStorage(tmp_path)` (refactor Step 5) — write them so that
# parametrize axis can be added without rewrites.
# -----------------------------------------------------------------------------


@pytest.fixture
def queue_aware_catalog(test_tmp_dir: Path, s3client: "S3Client") -> Catalog:
    # Reuse the catalog buckets from the module fixture but inject an in-memory
    # queue so we can observe arrival notifications without touching SQS.
    return Catalog(
        data_lake_bucket=DATA_LAKE_BUCKET,
        feature_store_bucket=FEATURE_STORE_BUCKET,
        persistent_store_bucket=PERSISTENT_STORE_BUCKET,
        schema_store=LocalSchemaStore(test_tmp_dir / "schemas"),
        queue=InMemoryQueue(),
    )


def _read_avro_via_s3(bucket: str, key: str) -> tuple[list[Any], dict[str, Any]]:
    """Round-trip helper: pull an avro blob out of S3 and decode it directly."""
    import io

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()

    reader = fastavro.reader(io.BytesIO(body))
    records: list[Any] = list(reader)
    metadata: dict[str, Any] = dict(reader.metadata)
    return records, metadata


class TestAvroRoundtrip:
    """Lock the avro encode/decode contract so refactor Step 5 (StoragePort + AvroSerializer
    extraction) can be checked for byte-level parity.
    """

    def test_primitive_types_roundtrip(self, queue_aware_catalog: Catalog, test_tmp_dir: Path) -> None:
        # Write a one-off schema that exercises strings, longs, doubles, booleans, nulls.
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

    def test_array_and_record_types_roundtrip(
        self, queue_aware_catalog: Catalog, test_tmp_dir: Path
    ) -> None:
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

    def test_metadata_written_to_avro(
        self, queue_aware_catalog: Catalog, test_tmp_dir: Path
    ) -> None:
        # The catalog writes the asset's to_metadata() into avro's metadata block.
        schema = {
            "type": "record",
            "name": "MetadataCheck",
            "fields": [{"name": "id", "type": "string"}],
        }
        schema_path = test_tmp_dir / "schemas" / "metadata-check.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        date = datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)
        asset = DataLakeAsset("metadata-check", date)
        queue_aware_catalog.store_asset(
            asset, [{"id": "x"}], notify=False, schema_path=schema_path.as_posix()
        )

        _, metadata = _read_avro_via_s3(DATA_LAKE_BUCKET, asset.get_path())
        # fastavro returns metadata keys as bytes.
        decoded = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                   for k, v in metadata.items()}
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
        queue_aware_catalog.store_asset(
            asset, [{"id": "x"}], notify=True, schema_path=schema_path.as_posix()
        )

        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        assert len(queue._messages) == 1
        message = queue._messages[0]
        assert isinstance(message, DataLakeArrivalMessage)
        assert message.asset == asset
        assert message.event == ArrivalEvent.ARRIVED
        assert message.schema_path == schema_path.as_posix()

    def test_notify_false_skips_message(
        self, queue_aware_catalog: Catalog, test_tmp_dir: Path
    ) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "notify-false.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        asset = DataLakeAsset("notify-false", utcnow())
        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        before = len(queue._messages)
        queue_aware_catalog.store_asset(
            asset, [{"id": "x"}], notify=False, schema_path=schema_path.as_posix()
        )
        assert len(queue._messages) == before

    def test_non_datalake_asset_does_not_notify(
        self, queue_aware_catalog: Catalog, test_tmp_dir: Path
    ) -> None:
        # FeatureAsset / PersistentStoreAsset should not trigger queue notifications even
        # when notify=True (per `_notify_asset_arrival`).
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "feature-no-notify.v1.avro.json"
        schema_path.write_text(json.dumps(schema))

        feature_asset = FeatureAsset("feature-no-notify", utcnow(), "1d")
        queue = queue_aware_catalog.queue
        assert isinstance(queue, InMemoryQueue)
        before = len(queue._messages)
        queue_aware_catalog.store_asset(
            feature_asset, [{"id": "x"}], notify=True, schema_path=schema_path.as_posix()
        )
        assert len(queue._messages) == before


class TestPartitionIterationRegex:
    """Pins the key-parsing contract of `iter_datalake_partitions`. Refactor F1
    (StoragePort) hands over the key iteration to the adapter; the regex must continue
    to skip non-conforming keys and parse valid ones.
    """

    def test_multiple_versions_for_same_date(self, default_catalog: Catalog, test_tmp_dir: Path) -> None:
        schema = {"type": "record", "name": "X", "fields": [{"name": "id", "type": "string"}]}
        schema_path = test_tmp_dir / "schemas" / "multiver.v1.avro.json"
        schema_path.write_text(json.dumps(schema))
        # Write the same asset name with two schema versions on the same date.
        date = datetime.datetime(2025, 6, 1, tzinfo=datetime.timezone.utc)
        default_catalog.store_asset(
            DataLakeAsset("multiver", date, schema_version=1),
            [{"id": "v1"}], notify=False, schema_path=schema_path.as_posix(),
        )
        default_catalog.store_asset(
            DataLakeAsset("multiver", date, schema_version=2),
            [{"id": "v2"}], notify=False, schema_path=schema_path.as_posix(),
        )

        partitions = list(default_catalog.iter_datalake_partitions("multiver"))
        versions = sorted(p.schema_version for p in partitions)
        assert versions == [1, 2]

    def test_skips_keys_with_invalid_filename(
        self, default_catalog: Catalog, s3client: "S3Client"
    ) -> None:
        # Drop a junk key under the asset prefix and verify iter_datalake_partitions
        # silently skips it.
        s3client.put_object(
            Bucket=DATA_LAKE_BUCKET,
            Key="junk-asset/date=2025-07-01T00:00:00+00:00/not-an-avro-file.txt",
            Body=b"junk",
        )
        # No partitions should match since the filename doesn't fit the version regex.
        partitions = list(default_catalog.iter_datalake_partitions("junk-asset"))
        assert partitions == []

    def test_skips_keys_with_wrong_structure(
        self, default_catalog: Catalog, s3client: "S3Client"
    ) -> None:
        # A key with fewer than 3 path segments must be skipped (no exception).
        s3client.put_object(
            Bucket=DATA_LAKE_BUCKET,
            Key="bad-shape-asset/v1.avro",
            Body=b"junk",
        )
        partitions = list(default_catalog.iter_datalake_partitions("bad-shape-asset"))
        assert partitions == []


class TestFeatureStorePartitionQuantization:
    @pytest.mark.usefixtures("_test_data")
    def test_skips_missing_partitions(self, default_catalog: Catalog) -> None:
        # Span a range that is wider than what's seeded; missing partitions must be
        # silently skipped, not raised.
        date_from = SUPERFEATURE_DATE_START - datetime.timedelta(days=14)
        date_to = SUPERFEATURE_PARTITION_DATES[-1] + datetime.timedelta(days=14)
        partitions = list(
            default_catalog.iter_feature_store_partitions(
                "superfeature", TimeResolution(1, "d"), date_from, date_to
            )
        )
        # Only the seeded partitions are returned; the surrounding dates aren't.
        assert {p.partition_date for p in partitions} == set(SUPERFEATURE_PARTITION_DATES)
