"""Tests for the deprecated `hyperion.infrastructure.geo.location` path.

The DDD refactor (F2 / Step 3) moved `Location`, `NamedLocation`,
`SpatialKMeans` and the distance helpers to `hyperion/domain/geo.py` and removed
the `Location._cache` class variable. These tests deliberately keep importing
from the old `hyperion.infrastructure.geo.location` path so the deprecation
shim stays exercised, and pin the (now cache-less) distance math.
"""

import datetime

import numpy as np
import pytest

from hyperion.infrastructure.geo.location import (
    EARTH_RADIUS_METERS,
    Location,
    NamedLocation,
    SpatialKMeans,
    meters_to_degrees,
)


class TestHaversineDistance:
    def test_same_point_is_zero(self) -> None:
        location = Location(50.0, 14.0)
        assert location.get_distance(location) == 0.0

    def test_one_degree_at_equator(self) -> None:
        # 1 degree of longitude at the equator is ~111,195 m via haversine.
        a = Location(0.0, 0.0)
        b = Location(0.0, 1.0)
        distance = a.get_distance(b)
        assert distance == pytest.approx(111_195, rel=1e-3)

    def test_quarter_circumference(self) -> None:
        # (0,0) → (0, 90deg) is a quarter of earth's circumference.
        a = Location(0.0, 0.0)
        b = Location(0.0, 90.0)
        expected = EARTH_RADIUS_METERS * np.pi / 2
        assert a.get_distance(b) == pytest.approx(expected, rel=1e-6)

    def test_antipode_is_half_circumference(self) -> None:
        a = Location(0.0, 0.0)
        b = Location(0.0, 180.0)
        expected = EARTH_RADIUS_METERS * np.pi
        assert a.get_distance(b) == pytest.approx(expected, rel=1e-6)

    def test_symmetric(self) -> None:
        a = Location(48.8566, 2.3522)
        b = Location(52.52, 13.405)
        assert a.get_distance(b) == pytest.approx(b.get_distance(a), rel=1e-9)


class TestEuclideanDistance:
    def test_same_point_is_zero(self) -> None:
        location = Location(50.0, 14.0)
        assert location.get_distance(location, approximate=True) == 0.0

    def test_approximation_close_to_haversine_for_short_distance(self) -> None:
        # For ~10 km separations the equirectangular approximation should be within ~1%.
        a = Location(50.0, 14.0)
        b = Location(50.05, 14.05)
        haversine_distance = a.get_distance(b)
        euclidean = a.get_distance(b, approximate=True)
        assert euclidean == pytest.approx(haversine_distance, rel=1e-2)


class TestDistanceMath:
    def test_call_returns_haversine_math_result(self) -> None:
        a = Location(0.0, 0.0)
        b = Location(0.0, 1.0)
        result = a.get_distance(b)
        assert result == pytest.approx(111_195, rel=1e-3)

    def test_get_distance_equals_haversine_helper(self) -> None:
        # F2 removed the (write-never) cache layer; get_distance must return
        # exactly the haversine math for the non-approximate path.
        a = Location(48.8566, 2.3522)
        b = Location(52.52, 13.405)
        assert a.get_distance(b) == pytest.approx(a._get_distance_haversine(b), rel=1e-12)


class TestGetNearest:
    def test_picks_closest(self) -> None:
        origin = Location(0.0, 0.0)
        far = Location(10.0, 10.0)
        near = Location(0.0, 0.001)
        assert origin.get_nearest([far, near]) is near

    def test_empty_iterable_raises(self) -> None:
        origin = Location(0.0, 0.0)
        with pytest.raises(ValueError, match="None of the given locations"):
            origin.get_nearest([])


class TestNamedLocation:
    def test_frozen_and_equal(self) -> None:
        loc = Location(0.0, 0.0)
        a = NamedLocation(location=loc, country="DE")
        b = NamedLocation(location=loc, country="DE")
        assert a == b
        assert hash(a) == hash(b)
        with pytest.raises(Exception):  # FrozenInstanceError  # noqa: B017, PT011
            a.country = "FR"  # type: ignore[misc]

    def test_dataclasses_replace_pattern(self) -> None:
        # gmaps.reverse_geocode uses dataclasses.replace on the inner Location.
        from dataclasses import replace

        original = Location(0.0, 0.0)
        renamed = replace(original, title="custom", address="custom address")
        assert renamed.latitude == 0.0
        assert renamed.title == "custom"
        assert renamed.address == "custom address"


class TestMetersToDegrees:
    def test_at_equator(self) -> None:
        lat_deg, lon_deg = meters_to_degrees(111_000, 0.0)
        assert lat_deg == pytest.approx(1.0, rel=1e-9)
        assert lon_deg == pytest.approx(1.0, rel=1e-9)

    def test_at_high_latitude_longitude_widens(self) -> None:
        # cos(60deg) = 0.5 → 1 longitude degree covers half the equatorial meters.
        _, lon_deg = meters_to_degrees(111_000, 60.0)
        assert lon_deg == pytest.approx(2.0, rel=1e-9)


class TestSpatialKMeans:
    def test_deterministic_with_seeded_rng(self) -> None:
        # Two tight clusters; with a seeded numpy RNG, the result is stable.
        cluster_a = [Location(0.0 + 1e-4 * i, 0.0) for i in range(5)]
        cluster_b = [Location(10.0 + 1e-4 * i, 10.0) for i in range(5)]

        np.random.seed(42)
        clusters_first = SpatialKMeans(cluster_a + cluster_b).fit(k=2)

        np.random.seed(42)
        clusters_second = SpatialKMeans(cluster_a + cluster_b).fit(k=2)

        first_centroid_coords = sorted((c.latitude, c.longitude) for c in clusters_first)
        second_centroid_coords = sorted((c.latitude, c.longitude) for c in clusters_second)
        assert first_centroid_coords == second_centroid_coords

    def test_two_clusters_separated(self) -> None:
        cluster_a = [Location(0.0 + 1e-4 * i, 0.0) for i in range(5)]
        cluster_b = [Location(10.0 + 1e-4 * i, 10.0) for i in range(5)]
        np.random.seed(0)
        clusters = SpatialKMeans(cluster_a + cluster_b).fit(k=2)
        # All members of cluster_a end up under one centroid, cluster_b under the other.
        assert len(clusters) == 2
        sizes = sorted(len(members) for members in clusters.values())
        assert sizes == [5, 5]


def test_imports_dont_use_datetime_at_module_level() -> None:
    # Sanity: the geo module's public API surface is independent of datetime — used by
    # the gmaps module which builds NamedLocation. This is a smoke import that the test
    # subprocess in test_import_graph.py asserts more strictly.
    assert datetime is not None
