"""Common location classes and functions."""

import math
from dataclasses import dataclass

LATITUDE_DEGREE_TO_METERS = 111_000


def meters_to_degrees(meters: float, at_latitude: float) -> tuple[float, float]:
    """Convert meters to degrees at a given latitude."""
    return meters / LATITUDE_DEGREE_TO_METERS, meters / (
        LATITUDE_DEGREE_TO_METERS * math.cos(math.radians(at_latitude))
    )


@dataclass(frozen=True, eq=True)
class Location:
    """A geographical location."""

    latitude: float
    longitude: float
    title: str | None = None
    address: str | None = None
