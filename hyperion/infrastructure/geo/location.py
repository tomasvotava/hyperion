"""Common location classes and functions."""

from dataclasses import dataclass


@dataclass
class Location:
    """A geographical location."""

    latitude: float
    longitude: float
    title: str | None = None
    address: str | None = None
