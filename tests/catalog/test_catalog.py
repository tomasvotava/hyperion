import datetime
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import pytest

from hyperion.catalog.catalog import Catalog
from hyperion.catalog.schema import LocalSchemaStore
from hyperion.dateutils import utcnow
from hyperion.entities.catalog import AssetProtocol, DataLakeAsset, FeatureAsset, PersistentStoreAsset
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


@pytest.fixture(name="s3client", scope="module")
def _s3client(_moto_server: None) -> "S3Client":
    s3 = boto3.client("s3")
    buckets = (DATA_LAKE_BUCKET, FEATURE_STORE_BUCKET, PERSISTENT_STORE_BUCKET, PREFIXED_BUCKET)
    for bucket in buckets:
        s3.create_bucket(Bucket=bucket)
    return s3


def _create_fake_data_lake(data_dir: Path, s3client: "S3Client") -> None:
    avro_file = (data_dir / "assets/users.v1.avro").resolve().as_posix()
    path = f"users/date={USERS_PARTITION_DATE.isoformat()}/v1.avro"
    s3client.upload_file(Filename=avro_file, Bucket=DATA_LAKE_BUCKET, Key=path)
    s3client.upload_file(Filename=avro_file, Bucket=PREFIXED_BUCKET, Key=f"{DATA_LAKE_PREFIX}path")


@pytest.fixture(scope="module")
def _test_data(data_dir: Path, s3client: "S3Client") -> None:
    _create_fake_data_lake(data_dir, s3client)


@pytest.fixture(name="test_tmp_dir", scope="session")
def _test_tmp_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="tmphyp_") as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "schemas").mkdir(parents=True)
        (tmp_path / "cache").mkdir(parents=True)
        yield tmp_path


@pytest.fixture(name="catalog", scope="session")
def _catalog(test_tmp_dir: Path) -> Catalog:
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


class TestCatalog:
    @pytest.mark.parametrize(
        ("asset", "expected_bucket", "expected_prefix"),
        [
            (DataLakeAsset("test", utcnow()), DATA_LAKE_BUCKET, DATA_LAKE_PREFIX),
            (FeatureAsset("test", utcnow(), "1d"), FEATURE_STORE_BUCKET, FEATURE_STORE_PREFIX),
            (PersistentStoreAsset("test"), PERSISTENT_STORE_BUCKET, PERSISTENT_STORE_PREFIX),
        ],
    )
    def test_store_config(
        self,
        catalog: Catalog,
        prefixed_catalog: Catalog,
        asset: AssetProtocol,
        expected_bucket: str,
        expected_prefix: str,
    ) -> None:
        store_config = catalog.get_store_config(asset)
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
