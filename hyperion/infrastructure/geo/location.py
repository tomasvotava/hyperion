"""Common location classes and functions."""

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar, Generic, TypeVar, cast

import haversine
import numpy as np
import numpy.typing as npt
from haversine.haversine import get_avg_earth_radius

from hyperion.infrastructure.cache import Cache
from hyperion.logging import get_logger

LATITUDE_DEGREE_TO_METERS = 111_000
EARTH_RADIUS_METERS = get_avg_earth_radius(haversine.Unit.METERS)

logger = get_logger("hyperion-geo")

AnyLocation = TypeVar("AnyLocation", bound="Location")


def meters_to_degrees(meters: float, at_latitude: float) -> tuple[float, float]:
    """Convert meters to degrees at a given latitude.

    Args:
        meters (float): The distance in meters.
        at_latitude (float): The latitude at which the conversion should be done.

    Returns:
        tuple[float, float]: The distance in degrees for latitude and longitude.
    """
    return meters / LATITUDE_DEGREE_TO_METERS, meters / (
        LATITUDE_DEGREE_TO_METERS * math.cos(math.radians(at_latitude))
    )


class SpatialKMeans(Generic[AnyLocation]):
    """K-means clustering for geographical locations."""

    def __init__(self, locations: Iterable[AnyLocation]) -> None:
        """Initialize the K-means clustering with the given locations.

        Args:
            locations (Iterable[Location]): The locations to cluster.
        """
        self.locations = list(locations)
        self.locations_array = np.array(
            [[location.latitude, location.longitude] for location in self.locations],
        )

    def _get_distances_from_centroids(self, centroids: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.array(
            [
                [haversine.haversine((point[0], point[1]), (centroid[0], centroid[1])) for centroid in centroids]
                for point in self.locations_array
            ]
        )

    def fit(self, k: int, max_iters: int = 100) -> dict["Location", list[AnyLocation]]:
        """Fit the K-means model with k clusters and return the clusters.

        Args:
            k (int): The desired number of clusters (must be less than the number of locations).
            max_iters (int, optional): The maximum number of iterations. Defaults to 100.

        Returns:
            dict[Location, list[Location]]: The clusters with the centroids as keys.
        """
        centroids = self.locations_array[np.random.choice(len(self.locations_array), k, replace=False), :2]

        for _ in range(max_iters):
            distances = self._get_distances_from_centroids(centroids)

            cluster_assignments = np.argmin(distances, axis=1)

            new_centroids_list = []
            for cluster_idx in range(k):
                cluster_points = self.locations_array[cluster_assignments == cluster_idx, :2]
                if len(cluster_points) > 0:
                    new_centroids_list.append(cluster_points.mean(axis=0))
                else:
                    new_centroids_list.append(centroids[cluster_idx])  # Keep old centroid for empty clusters
            new_centroids = np.array(new_centroids_list)

            if np.allclose(centroids, new_centroids):
                break
            centroids = new_centroids

        clusters: dict[Location, list[AnyLocation]] = defaultdict(list)
        centroid_locations = [Location(float(lat), float(lon)) for lat, lon in centroids]

        for location_id, centroid_id in enumerate(cluster_assignments):
            clusters[centroid_locations[centroid_id]].append(self.locations[location_id])
        return dict(clusters)


@dataclass(frozen=True, eq=True)
class Location:
    """A geographical location."""

    _cache: ClassVar[Cache | None] = None

    latitude: float
    longitude: float
    title: str | None = None
    address: str | None = None

    @classmethod
    def _get_cache(cls) -> Cache:
        if cls._cache is None:
            cls._cache = Cache.from_config()
        return cls._cache

    def _get_distance_haversine(self, other: "Location") -> float:
        return cast(
            float,
            haversine.haversine(
                (self.latitude, self.longitude), (other.latitude, other.longitude), haversine.Unit.METERS
            ),
        )

    def _get_distance_euclidean(self, other: "Location") -> float:
        lat_diff = math.radians(self.latitude - other.latitude)
        lon_diff = math.radians(self.longitude - other.longitude)
        x = lon_diff * math.cos(math.radians((self.latitude + other.latitude) / 2))
        y = lat_diff
        return cast(float, EARTH_RADIUS_METERS) * math.sqrt(x**2 + y**2)

    def get_distance(self, other: "Location", approximate: bool = False) -> float:
        """Get the distance to another location in meters.
        If approximate is False (default), will use haversine. Otherwise uses Euclidean to approximate the distance.

        Args:
            other (Location): The other location.
            approximate (bool, optional): Whether to approximate the distance. Defaults to False.

        Returns:
            float: The distance in meters.
        """
        cache_key = str((self.latitude, self.longitude, other.latitude, other.longitude, approximate))
        cache = self._get_cache()
        if cached := cache.get(cache_key):
            return float(cached)
        if approximate:
            return self._get_distance_euclidean(other)
        return self._get_distance_haversine(other)

    def get_nearest(
        self, others: Iterable[AnyLocation], threshold: float | None = None, approximate: bool = False
    ) -> AnyLocation:
        """Get the closest location from the iterable of locations.

        If threshold is given and all locations are further than the threshold in meters, an ValueError is raised.

        Args:
            others (Iterable[Location]): The other locations.
            threshold (float, optional): The maximum distance in meters. Defaults to None.
            approximate (bool, optional): Whether to approximate the distance. Defaults to False.

        Returns:
            Location: The nearest location.
        """
        nearest: tuple[AnyLocation, float] | None = None
        for other in others:
            distance = self.get_distance(other, approximate=approximate)
            if nearest is None or nearest[1] > distance:
                nearest = (other, distance)
        if nearest is None:
            raise ValueError(f"None of the given locations is close enough to {self!r}.")
        logger.debug("Found nearest location.", this=self, other=nearest[0], distance=nearest[1])
        return nearest[0]


@dataclass(frozen=True, eq=True)
class NamedLocation:
    location: Location
    route: str | None = None
    neighborhood: str | None = None
    sublocality: str | None = None
    administrative_area: str | None = None
    administrative_area_level_2: str | None = None
    country: str | None = None
    address: str | None = None
