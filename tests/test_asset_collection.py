import datetime
from collections.abc import Iterator
from typing import Any, ClassVar, cast

import pytest
from pydantic import BaseModel

from hyperion import dateutils
from hyperion.catalog.catalog import Catalog
from hyperion.dateutils import TimeResolution, iter_dates_between
from hyperion.entities.catalog import FeatureAsset, FeatureModel
from hyperion.repository.asset_collection import (
    AssetCollection,
    FeatureAssetSpecification,
    FeatureFetchSpecifier,
    _CollectionState,
    _FeatureFetchSpecifier,
)
from hyperion.typeutils import DateOrDelta

THE_NOW = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)


class _FakeCatalog:
    def iter_feature_store_partitions(
        self,
        asset_name: str,
        resolution: TimeResolution,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        schema_version: int = 1,
    ) -> Iterator[FeatureAsset]:
        for partition_date in iter_dates_between(start_date, end_date, resolution.unit):
            yield FeatureAsset(
                asset_name, partition_date=partition_date, resolution=resolution, schema_version=schema_version
            )

    def retrieve_asset(self, asset: FeatureAsset) -> Iterator[dict[str, Any]]:
        return iter([{"partition_date": asset.partition_date}])


class MockFeature(FeatureModel, BaseModel):
    asset_name: ClassVar = "MockedAsset"
    resolution: ClassVar = TimeResolution(1, "d")
    schema_version: ClassVar = 1

    partition_date: datetime.datetime


@pytest.fixture(scope="module", autouse=True)
def _deterministic_utcnow() -> Iterator[None]:
    monkeypatch = pytest.MonkeyPatch().context()
    utcnow_occurences = ("hyperion.dateutils.utcnow", "hyperion.repository.asset_collection.utcnow")
    with monkeypatch as mp:
        for name in utcnow_occurences:
            mp.setattr(name, lambda: THE_NOW)
        assert dateutils.utcnow() == THE_NOW
        yield
    assert dateutils.utcnow() != THE_NOW


class TestAssetCollection:
    @pytest.mark.parametrize(
        ("start_date", "end_date", "expected_start_date", "expected_end_date"),
        [
            (None, None, datetime.datetime.min, THE_NOW),
            (
                datetime.timedelta(days=-1),
                datetime.timedelta(days=1),
                THE_NOW - datetime.timedelta(days=1),
                THE_NOW + datetime.timedelta(days=1),
            ),
            (
                datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
                None,
                datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
                THE_NOW,
            ),
        ],
    )
    def test_resolve_feature_asset_specification_dates(
        self,
        start_date: DateOrDelta | None,
        end_date: DateOrDelta | None,
        expected_start_date: datetime.datetime,
        expected_end_date: datetime.datetime,
    ) -> None:
        specs = FeatureAssetSpecification(MockFeature, start_date, end_date)
        assert specs.resolve_start_date() == expected_start_date
        assert specs.resolve_end_date() == expected_end_date

    def test_collection_fetch_registration(self) -> None:
        class _MockCollection(AssetCollection):
            mocks = FeatureFetchSpecifier(MockFeature, datetime.timedelta(days=-10), datetime.timedelta(days=10))

        assert hasattr(_MockCollection, "_state"), "Collection should have _state after fields registration"
        assert isinstance(state := _MockCollection._state, _CollectionState)
        assert state.fetched is _MockCollection.is_fetched()
        assert state.fetched is False
        assert isinstance(mocks_field := state.fetch_specifications["mocks"], FeatureAssetSpecification)
        assert mocks_field.resolve_start_date() == THE_NOW - datetime.timedelta(days=10)
        assert mocks_field.resolve_end_date() == THE_NOW + datetime.timedelta(days=10)
        assert isinstance(descriptor := _MockCollection.__dict__["mocks"], _FeatureFetchSpecifier)
        assert descriptor.field_name == "mocks"
        assert descriptor.owner is _MockCollection

    def test_descriptor_raises_before_fetch(self) -> None:
        class _MockCollection(AssetCollection):
            mocks = FeatureFetchSpecifier(MockFeature)

        with pytest.raises(RuntimeError, match="Owner collection '_MockCollection' was not fetched yet."):
            _ = _MockCollection().mocks

    async def test_fetch_all(self) -> None:
        class _MockCollection(AssetCollection):
            catalog = cast(Catalog, _FakeCatalog())
            mocks = FeatureFetchSpecifier(MockFeature, datetime.timedelta(days=-10))

        await _MockCollection.fetch_all()
        assert _MockCollection.is_fetched(), "is_fetched flag should be set after successful fetch"
        assert len(_MockCollection.mocks) == 11
        expected_dates = {THE_NOW - datetime.timedelta(days=d) for d in range(11)}
        assert {mock.partition_date for mock in _MockCollection.mocks} == expected_dates
