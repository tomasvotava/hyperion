import datetime
from collections.abc import Iterator
from typing import Annotated, Any, ClassVar, cast

import pandera.errors
import pandera.typing as pt
import pytest
from pandera.engines.polars_engine import DateTime
from pydantic import BaseModel

from hyperion import dateutils
from hyperion.catalog.catalog import Catalog
from hyperion.dateutils import TimeResolution, iter_dates_between
from hyperion.entities.catalog import FeatureAsset, FeatureModel, PolarsFeatureModel
from hyperion.repository.asset_collection import (
    AssetCollection,
    FeatureAssetSpecification,
    FeatureFetchSpecifier,
    PolarsFeatureFetchSpecifier,
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


class PolarsMockFeature(PolarsFeatureModel):
    _asset_name: ClassVar = "MockedAsset"
    _resolution: ClassVar = TimeResolution(1, "d")
    _schema_version: ClassVar = 1

    partition_date: pt.Series[Annotated[DateTime, False, "UTC", "us"]]


class PolarsMockFeatureInvalid(PolarsFeatureModel):
    _asset_name: ClassVar = "MockedAsset"
    _resolution: ClassVar = TimeResolution(1, "d")

    partition_date: pt.Series[str]


class MockCollection(AssetCollection):
    catalog = cast(Catalog, _FakeCatalog())
    mocks = FeatureFetchSpecifier(MockFeature, datetime.timedelta(days=-10), datetime.timedelta(days=10))


class PolarsMockCollection(AssetCollection):
    catalog = cast(Catalog, _FakeCatalog())
    mocks = PolarsFeatureFetchSpecifier(PolarsMockFeature, datetime.timedelta(days=-10), datetime.timedelta(days=10))


class PolarsMockCollectionInvalid(AssetCollection):
    catalog = cast(Catalog, _FakeCatalog())
    mocks = PolarsFeatureFetchSpecifier(
        PolarsMockFeatureInvalid, datetime.timedelta(days=-10), datetime.timedelta(days=10)
    )


class MixedMockCollection(AssetCollection):
    catalog = cast(Catalog, _FakeCatalog())
    mocks_pydantic = FeatureFetchSpecifier(MockFeature, datetime.timedelta(days=-10), datetime.timedelta(days=10))
    mocks_polars = PolarsFeatureFetchSpecifier(
        PolarsMockFeature, datetime.timedelta(days=-10), datetime.timedelta(days=10)
    )


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
    @pytest.mark.parametrize("feature_cls", [MockFeature, PolarsMockFeature], ids=lambda cls: cls.__name__)
    @pytest.mark.parametrize(
        ("start_date", "end_date", "expected_start_date", "expected_end_date"),
        [
            (None, None, datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), THE_NOW),
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
        feature_cls: type[MockFeature] | type[PolarsMockFeature],
        start_date: DateOrDelta | None,
        end_date: DateOrDelta | None,
        expected_start_date: datetime.datetime,
        expected_end_date: datetime.datetime,
    ) -> None:
        specs = FeatureAssetSpecification(feature_cls, start_date, end_date)
        assert specs.resolve_start_date() == expected_start_date
        assert specs.resolve_end_date() == expected_end_date

    @pytest.mark.parametrize("collection", [MockCollection, PolarsMockCollection], ids=lambda cls: cls.__name__)
    def test_collection_fetch_registration(self, collection: type[MockCollection] | type[PolarsMockCollection]) -> None:
        assert hasattr(collection, "_state"), "Collection should have _state after fields registration"
        assert isinstance(state := collection._state, _CollectionState)
        assert state.fetched is collection.is_fetched()
        assert state.fetched is False
        assert isinstance(mocks_field := state.fetch_specifications["mocks"], FeatureAssetSpecification)
        assert mocks_field.resolve_start_date() == THE_NOW - datetime.timedelta(days=10)
        assert mocks_field.resolve_end_date() == THE_NOW + datetime.timedelta(days=10)
        assert isinstance(descriptor := collection.__dict__["mocks"], _FeatureFetchSpecifier)
        assert descriptor.field_name == "mocks"
        assert descriptor.owner is collection

    @pytest.mark.parametrize("collection", [MockCollection, PolarsMockCollection], ids=lambda cls: cls.__name__)
    def test_descriptor_raises_before_fetch(
        self, collection: type[MockCollection] | type[PolarsMockCollection]
    ) -> None:
        with pytest.raises(
            RuntimeError, match="Owner collection '(MockCollection|PolarsMockCollection)' was not fetched yet."
        ):
            _ = collection().mocks

    async def test_fetch_all_pydantic(self) -> None:
        class _MockCollection(AssetCollection):
            catalog = cast(Catalog, _FakeCatalog())
            mocks = FeatureFetchSpecifier(MockFeature, datetime.timedelta(days=-10))

        await _MockCollection.fetch_all()
        assert _MockCollection.is_fetched(), "is_fetched flag should be set after successful fetch"
        assert len(_MockCollection.mocks) == 11
        expected_dates = {THE_NOW - datetime.timedelta(days=d) for d in range(11)}
        assert {mock.partition_date for mock in _MockCollection.mocks} == expected_dates

    async def test_fetch_all_polars(self) -> None:
        class _MockCollection(AssetCollection):
            catalog = cast(Catalog, _FakeCatalog())
            mocks = PolarsFeatureFetchSpecifier(PolarsMockFeature, datetime.timedelta(days=-10))

        await _MockCollection.fetch_all()
        assert _MockCollection.is_fetched(), "is_fetched flag should be set after successful fetch"
        assert len(_MockCollection.mocks.collect()) == 11
        expected_dates = {THE_NOW - datetime.timedelta(days=d) for d in range(11)}
        assert {mock for mock in _MockCollection.mocks.collect()["partition_date"]} == expected_dates

    async def test_fetch_polars_invalid(self) -> None:
        with pytest.raises(
            pandera.errors.SchemaError, match="expected column 'partition_date' to have type String, got Datetime"
        ):
            await PolarsMockCollectionInvalid.fetch_all()

    async def test_mixed_collection(self) -> None:
        await MixedMockCollection.fetch_all()
        assert len(MixedMockCollection.mocks_polars.collect()) == len(MixedMockCollection.mocks_pydantic)
        assert list(MixedMockCollection.mocks_polars.collect()["partition_date"]) == list(
            row.partition_date for row in MixedMockCollection.mocks_pydantic
        )
