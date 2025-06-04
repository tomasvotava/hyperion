import datetime
import json
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import pytest

from hyperion.catalog.catalog import AssetNotFoundError, Catalog
from hyperion.catalog.schema import LocalSchemaStore
from hyperion.dateutils import TimeResolution, assure_timezone, quantize_datetime, utcnow
from hyperion.entities.catalog import AssetProtocol, AssetType, DataLakeAsset, FeatureAsset, PersistentStoreAsset
from hyperion.infrastructure.cache import LocalFileCache

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
        partition_date = assure_timezone(datetime.date.fromisoformat(partition_date_str))
        _create_fake_asset("superfeature.1d", s3client, partition_date, "feature", feature_file)


@pytest.fixture(scope="module")
def _test_data(data_dir: Path, s3client: "S3Client") -> None:
    _create_fake_data_lake(data_dir, s3client)
    _create_fake_feature_store(data_dir, s3client)


@pytest.fixture(name="test_tmp_dir", scope="session")
def _test_tmp_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="tmphyp_") as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "schemas").mkdir(parents=True)
        (tmp_path / "cache").mkdir(parents=True)
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
                key: datetime.datetime.fromisoformat(value).astimezone(datetime.timezone.utc)
                if key == "date"
                else value
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
        with pytest.raises(AssetNotFoundError, match="Asset 'foobar' not found in the catalog."):
            next(data)

    def test_get_size_nonexistent(self, catalog: Catalog) -> None:
        foobar = DataLakeAsset("foobar", utcnow())
        with pytest.raises(AssetNotFoundError, match="Asset 'foobar' not found in the catalog."):
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
