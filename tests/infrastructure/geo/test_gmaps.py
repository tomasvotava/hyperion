"""Tests for `hyperion.infrastructure.geo.gmaps`.

The DDD refactor (F3 + F4) deletes `PersistentCache` and replaces it with an
injected `KeyValueStore` in `GoogleMaps.__init__`. These tests pin:
  - The geocode cache-shape contract (so existing 1.x serialised caches stay readable).
  - The Google Maps API surface (geocode / reverse_geocode / get_altitude) without
    touching the real network.
  - The singleton + context-manager behaviour.
"""

import json
from collections.abc import Iterable, Iterator
from typing import Any

import pytest

from hyperion.catalog.catalog import AssetNotFoundError, Catalog
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.infrastructure.cache import PersistentCache
from hyperion.infrastructure.geo.gmaps import GoogleMaps, _find_info_by_type
from hyperion.infrastructure.geo.location import Location, NamedLocation

GMAPS_API_KEY_ENV = "HYPERION_GEO_GMAPS_API_KEY"  # pragma: allowlist secret


class FakeCatalog:
    """An in-memory catalog that implements just enough surface for PersistentCache."""

    def __init__(self) -> None:
        self.stored: dict[str, list[dict[str, Any]]] = {}
        self.store_calls = 0
        self.retrieve_calls = 0

    def retrieve_asset(self, asset: PersistentStoreAsset) -> Iterator[dict[str, Any]]:
        self.retrieve_calls += 1
        key = asset.get_path()
        if key not in self.stored:
            raise AssetNotFoundError(asset)
        yield from self.stored[key]

    def store_asset(
        self,
        asset: PersistentStoreAsset,
        data: Iterable[dict[str, Any]],
        notify: bool = True,
        schema_path: str | None = None,
    ) -> None:
        self.store_calls += 1
        self.stored[asset.get_path()] = list(data)


class FakeGmapsClient:
    """Stub for googlemaps.Client; records calls so cache hit/miss can be asserted."""

    def __init__(
        self,
        *,
        geocode_results: list[dict[str, Any]] | None = None,
        reverse_results: list[dict[str, Any]] | None = None,
        elevation_results: list[dict[str, Any]] | None = None,
    ) -> None:
        self.geocode_results = geocode_results or []
        self.reverse_results = reverse_results or []
        self.elevation_results = elevation_results or []
        self.geocode_calls: list[str] = []
        self.reverse_calls: list[tuple[dict[str, Any], str | None]] = []
        self.elevation_calls: list[tuple[float, float]] = []

    def geocode(self, address: str) -> list[dict[str, Any]]:
        self.geocode_calls.append(address)
        return self.geocode_results

    def reverse_geocode(self, location: dict[str, Any], language: str | None = None) -> list[dict[str, Any]]:
        self.reverse_calls.append((location, language))
        return self.reverse_results

    def elevation(self, point: tuple[float, float]) -> list[dict[str, Any]]:
        self.elevation_calls.append(point)
        return self.elevation_results


@pytest.fixture(autouse=True)
def _reset_singleton() -> Iterator[None]:
    previous = GoogleMaps._instance
    GoogleMaps._instance = None
    yield
    GoogleMaps._instance = previous


@pytest.fixture
def fake_catalog(monkeypatch: pytest.MonkeyPatch) -> FakeCatalog:
    catalog = FakeCatalog()
    monkeypatch.setattr(Catalog, "from_config", classmethod(lambda cls: catalog))
    return catalog


def _build_gmaps(client: FakeGmapsClient) -> GoogleMaps:
    gmaps = GoogleMaps.__new__(GoogleMaps)
    gmaps.geocode_cache = PersistentCache(
        "gmaps", hash_keys=False, asset=PersistentStoreAsset("GEOCodeCache", schema_version=1)
    )
    gmaps.client = client
    gmaps._cache_context = None
    return gmaps


class TestGeocode:
    def test_cache_miss_calls_client_and_stores(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(
            geocode_results=[
                {
                    "geometry": {"location": {"lat": 48.8566, "lng": 2.3522}},
                    "formatted_address": "Paris, France",
                }
            ]
        )
        gmaps = _build_gmaps(client)
        with gmaps:
            location = gmaps.geocode("Paris")
        assert isinstance(location, Location)
        assert location.latitude == pytest.approx(48.8566)
        assert location.longitude == pytest.approx(2.3522)
        assert location.title == "Paris"
        assert location.address == "Paris, France"
        assert client.geocode_calls == ["Paris"]

    def test_cache_hit_avoids_client_call(self, fake_catalog: FakeCatalog) -> None:
        # Pre-populate the catalog so __enter__ loads the cache with our payload.
        # PersistentCache is constructed with prefix="gmaps", hash_keys=False, so the
        # internal cache key includes the prefix.
        cached_location = Location(latitude=1.0, longitude=2.0, title="Cached", address="Cached addr")
        fake_catalog.stored[PersistentStoreAsset("GEOCodeCache", schema_version=1).get_path()] = [
            {
                "key": "gmaps:Cached",
                "value": json.dumps({"latitude": 1.0, "longitude": 2.0, "title": "Cached", "address": "Cached addr"}),
            }
        ]
        client = FakeGmapsClient()  # any client call would fail (empty response → ValueError)
        gmaps = _build_gmaps(client)
        with gmaps:
            result = gmaps.geocode("Cached")
        assert result == cached_location
        assert client.geocode_calls == []

    def test_cached_json_shape(self, fake_catalog: FakeCatalog) -> None:
        # The persisted JSON keys define the on-disk format for downstream consumers.
        # Post-refactor, the new KeyValueStore-backed cache must persist the same shape.
        client = FakeGmapsClient(
            geocode_results=[
                {
                    "geometry": {"location": {"lat": 0.0, "lng": 0.0}},
                    "formatted_address": "Null Island",
                }
            ]
        )
        gmaps = _build_gmaps(client)
        with gmaps:
            gmaps.geocode("origin")
        rows = fake_catalog.stored[PersistentStoreAsset("GEOCodeCache", schema_version=1).get_path()]
        assert len(rows) == 1
        # Internal cache key is prefixed; the lookup address was "origin".
        assert rows[0]["key"] == "gmaps:origin"
        payload = json.loads(rows[0]["value"])
        assert set(payload.keys()) == {"latitude", "longitude", "title", "address"}
        assert payload["title"] == "origin"
        assert payload["address"] == "Null Island"

    def test_empty_result_raises(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(geocode_results=[])
        gmaps = _build_gmaps(client)
        with gmaps, pytest.raises(ValueError, match="Could not geocode address"):
            gmaps.geocode("nowhere")


class TestReverseGeocode:
    def test_extracts_address_components(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(
            reverse_results=[
                {
                    "formatted_address": "10 Downing St, London SW1A 2AA, UK",
                    "address_components": [
                        {"long_name": "10", "short_name": "10", "types": ["street_number"]},
                        {"long_name": "Downing Street", "short_name": "Downing St", "types": ["route"]},
                        {"long_name": "Westminster", "short_name": "Westminster", "types": ["neighborhood"]},
                        {"long_name": "London", "short_name": "London", "types": ["sublocality"]},
                        {
                            "long_name": "Greater London",
                            "short_name": "Greater London",
                            "types": ["administrative_area_level_2"],
                        },
                        {"long_name": "England", "short_name": "England", "types": ["administrative_area_level_1"]},
                        {"long_name": "United Kingdom", "short_name": "UK", "types": ["country"]},
                    ],
                }
            ]
        )
        gmaps = _build_gmaps(client)
        location = Location(latitude=51.5034, longitude=-0.1276)
        named = gmaps.reverse_geocode(location)
        assert isinstance(named, NamedLocation)
        assert named.route == "Downing Street"
        assert named.neighborhood == "Westminster"
        assert named.sublocality == "London"
        assert named.administrative_area == "England"
        assert named.administrative_area_level_2 == "Greater London"
        assert named.country == "United Kingdom"
        assert named.address == "10 Downing St, London SW1A 2AA, UK"
        # Inner Location was rebuilt via dataclasses.replace; coords preserved.
        assert named.location.latitude == pytest.approx(51.5034)
        assert named.location.longitude == pytest.approx(-0.1276)
        # title is set to the route name
        assert named.location.title == "Downing Street"

    def test_passes_language(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(reverse_results=[{"formatted_address": "Some Address", "address_components": []}])
        gmaps = _build_gmaps(client)
        gmaps.reverse_geocode(Location(0.0, 0.0), language="de")
        assert client.reverse_calls == [({"lat": 0.0, "lng": 0.0}, "de")]

    def test_empty_results_raises(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(reverse_results=[])
        gmaps = _build_gmaps(client)
        with pytest.raises(ValueError, match="no results"):
            gmaps.reverse_geocode(Location(0.0, 0.0))


class TestGetAltitude:
    def test_returns_float(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(elevation_results=[{"elevation": 123.45}])
        gmaps = _build_gmaps(client)
        assert gmaps.get_altitude(Location(0.0, 0.0)) == pytest.approx(123.45)

    def test_empty_raises(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(elevation_results=[])
        gmaps = _build_gmaps(client)
        with pytest.raises(ValueError, match="No elevation data"):
            gmaps.get_altitude(Location(0.0, 0.0))


class TestFromConfig:
    def test_raises_when_api_key_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(GMAPS_API_KEY_ENV, raising=False)
        with pytest.raises(ValueError, match="Google Maps API key is not set"):
            GoogleMaps.from_config()

    def test_singleton(self, monkeypatch: pytest.MonkeyPatch, fake_catalog: FakeCatalog) -> None:
        # googlemaps.Client validates the key prefix; use a syntactically valid one.
        monkeypatch.setenv(GMAPS_API_KEY_ENV, "AIzaTestKeyForSingletonCheck")
        first = GoogleMaps.from_config()
        second = GoogleMaps.from_config()
        assert first is second


class TestContextManager:
    def test_exit_flushes_cache_to_catalog(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient(
            geocode_results=[{"geometry": {"location": {"lat": 0.0, "lng": 0.0}}, "formatted_address": "x"}]
        )
        gmaps = _build_gmaps(client)
        before = fake_catalog.store_calls
        with gmaps:
            gmaps.geocode("x")
        # __exit__ on the PersistentCache calls store_asset exactly once.
        assert fake_catalog.store_calls == before + 1

    def test_exit_without_enter_is_noop(self, fake_catalog: FakeCatalog) -> None:
        client = FakeGmapsClient()
        gmaps = _build_gmaps(client)
        # Never entered; exit must not raise even though cache_context is None.
        gmaps.__exit__(None, None, None)


class TestFindInfoByType:
    def test_picks_long_name(self) -> None:
        components = [
            {"long_name": "Berlin", "short_name": "BE", "types": ["administrative_area_level_1"]},
        ]
        assert _find_info_by_type(components, "administrative_area_level_1") == "Berlin"

    def test_falls_back_to_short_name(self) -> None:
        components = [{"short_name": "BE", "types": ["administrative_area_level_1"]}]
        assert _find_info_by_type(components, "administrative_area_level_1") == "BE"

    def test_returns_none_when_type_missing(self) -> None:
        components = [{"long_name": "Berlin", "types": ["administrative_area_level_1"]}]
        assert _find_info_by_type(components, "country") is None

    def test_invalid_types_field_raises(self) -> None:
        with pytest.raises(TypeError, match="component type info"):
            _find_info_by_type([{"long_name": "x", "types": "not a list"}], "country")
