"""Direct unit tests for asset identity classes.

These tests pin the path-building / metadata contract that the upcoming DDD refactor
will split between `hyperion/domain/assets.py` (pure pydantic identity) and
`hyperion/data/asset_schemas.py` (pandera-backed validation). The identity layer
must round-trip identically before and after the split.
"""

import datetime
import json

import pytest

from hyperion.dateutils import TimeResolution
from hyperion.entities.catalog import (
    DataLakeAsset,
    FeatureAsset,
    PersistentStoreAsset,
    get_prefixed_path,
)

UTC = datetime.timezone.utc


class TestGetPrefixedPath:
    def test_empty_prefix(self) -> None:
        assert get_prefixed_path("foo/bar") == "foo/bar"

    def test_no_prefix_explicit(self) -> None:
        assert get_prefixed_path("foo/bar", "") == "foo/bar"

    def test_simple_prefix(self) -> None:
        assert get_prefixed_path("foo/bar", "data") == "data/foo/bar"

    def test_prefix_trailing_slash_stripped(self) -> None:
        assert get_prefixed_path("foo/bar", "data/") == "data/foo/bar"

    def test_prefix_leading_slash_stripped(self) -> None:
        assert get_prefixed_path("foo/bar", "/data") == "data/foo/bar"

    def test_prefix_both_slashes_stripped(self) -> None:
        assert get_prefixed_path("foo/bar", "/data/") == "data/foo/bar"


class TestDataLakeAsset:
    def test_asset_type(self) -> None:
        assert DataLakeAsset.asset_type == "data_lake"

    def test_defaults(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC))
        assert asset.name == "users"
        assert asset.schema_version == 1

    def test_get_path_aware_date(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC))
        assert asset.get_path() == "users/date=2025-01-02T03:04:05+00:00/v1.avro"

    def test_get_path_with_prefix(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC))
        assert asset.get_path("data-lake") == "data-lake/users/date=2025-01-02T00:00:00+00:00/v1.avro"

    def test_get_path_higher_version(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC), schema_version=3)
        assert asset.get_path() == "users/date=2025-01-02T00:00:00+00:00/v3.avro"

    def test_get_path_naive_date_assumed_utc(self) -> None:
        # Intentionally naive datetime here to verify assure_timezone fallback.
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2))  # noqa: DTZ001
        assert asset.get_path() == "users/date=2025-01-02T00:00:00+00:00/v1.avro"

    def test_get_path_non_utc_date_converted(self) -> None:
        prague = datetime.timezone(datetime.timedelta(hours=2))
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, 5, tzinfo=prague))
        assert asset.get_path() == "users/date=2025-01-02T03:00:00+00:00/v1.avro"

    def test_to_metadata(self) -> None:
        date = datetime.datetime(2025, 1, 2, tzinfo=UTC)
        asset = DataLakeAsset("users", date, schema_version=2)
        assert asset.to_metadata() == {
            "name": "users",
            "date": date.isoformat(),
            "schema_version": "2",
        }

    def test_frozen_dataclass(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC))
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError  # noqa: B017, PT011
            asset.name = "other"  # type: ignore[misc]

    def test_equality_and_hash(self) -> None:
        date = datetime.datetime(2025, 1, 2, tzinfo=UTC)
        a = DataLakeAsset("users", date)
        b = DataLakeAsset("users", date)
        c = DataLakeAsset("users", date, schema_version=2)
        assert a == b
        assert hash(a) == hash(b)
        assert a != c


class TestPersistentStoreAsset:
    def test_asset_type(self) -> None:
        assert PersistentStoreAsset.asset_type == "persistent_store"

    def test_get_path_default_version(self) -> None:
        assert PersistentStoreAsset("GEOCodeCache").get_path() == "GEOCodeCache/v1.avro"

    def test_get_path_higher_version(self) -> None:
        assert PersistentStoreAsset("Cache", schema_version=7).get_path() == "Cache/v7.avro"

    def test_get_path_with_prefix(self) -> None:
        assert PersistentStoreAsset("Cache").get_path("ps") == "ps/Cache/v1.avro"

    def test_to_metadata(self) -> None:
        assert PersistentStoreAsset("Cache", 2).to_metadata() == {"name": "Cache", "schema_version": "2"}


class TestFeatureAsset:
    def test_asset_type(self) -> None:
        assert FeatureAsset.asset_type == "feature"

    def test_resolution_string_is_coerced(self) -> None:
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), "1d")
        assert isinstance(asset.resolution, TimeResolution)
        assert asset.resolution == TimeResolution(1, "d")

    def test_resolution_object_passthrough(self) -> None:
        resolution = TimeResolution(15, "m")
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), resolution)
        assert asset.resolution is resolution

    def test_feature_name_format(self) -> None:
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), "1d")
        assert asset.feature_name == "foo.1d"

    def test_get_path_without_partition_keys(self) -> None:
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), "1d")
        assert asset.get_path() == "foo.1d/partition_date=2025-01-02T00:00:00+00:00/v1.avro"

    def test_get_path_with_partition_keys_sorted(self) -> None:
        # Partition keys are emitted in sorted-key order.
        asset = FeatureAsset(
            "foo",
            datetime.datetime(2025, 1, 2, tzinfo=UTC),
            "1d",
            partition_keys={"region": "eu", "asset_class": "energy"},
        )
        path = asset.get_path()
        assert path == ("foo.1d/asset_class=energy/region=eu/partition_date=2025-01-02T00:00:00+00:00/v1.avro")

    def test_get_path_with_prefix(self) -> None:
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), "1d")
        assert asset.get_path("features") == "features/foo.1d/partition_date=2025-01-02T00:00:00+00:00/v1.avro"

    def test_get_path_higher_version(self) -> None:
        asset = FeatureAsset("foo", datetime.datetime(2025, 1, 2, tzinfo=UTC), "1d", schema_version=4)
        assert asset.get_path() == "foo.1d/partition_date=2025-01-02T00:00:00+00:00/v4.avro"

    def test_to_metadata_serializes_partition_keys_as_json(self) -> None:
        date = datetime.datetime(2025, 1, 2, tzinfo=UTC)
        asset = FeatureAsset("foo", date, "1d", partition_keys={"region": "eu"})
        metadata = asset.to_metadata()
        assert metadata["name"] == "foo"
        assert metadata["partition_date"] == date.isoformat()
        assert metadata["schema_version"] == "1"
        assert json.loads(metadata["partition_keys"]) == {"region": "eu"}

    def test_equality_with_string_resolution(self) -> None:
        date = datetime.datetime(2025, 1, 2, tzinfo=UTC)
        a = FeatureAsset("foo", date, "1d")
        b = FeatureAsset("foo", date, TimeResolution(1, "d"))
        assert a == b
