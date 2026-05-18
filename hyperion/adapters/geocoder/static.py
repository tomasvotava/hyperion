"""Static :class:`~hyperion.ports.geocoder.Geocoder` adapter (lite).

A dependency-free, offline geocoder backed by in-memory lookup tables. Intended
for tests and offline scenarios where the Google Maps API is unavailable. Its
error messages mirror :class:`~hyperion.adapters.geocoder.google.GoogleMaps` so
the two are substitutable behind the :class:`~hyperion.ports.geocoder.Geocoder`
port.
"""

from __future__ import annotations

from collections.abc import Mapping

from hyperion.domain.geo import Location, NamedLocation


class StaticGeocoder:
    """A geocoder that resolves from fixed lookup tables.

    Args:
        locations: Maps an address string to its :class:`Location`.
        named_locations: Maps a ``(latitude, longitude)`` tuple to a
            :class:`NamedLocation` for reverse geocoding.
        altitudes: Maps a ``(latitude, longitude)`` tuple to an altitude in
            meters above sea level.
    """

    def __init__(
        self,
        *,
        locations: Mapping[str, Location] | None = None,
        named_locations: Mapping[tuple[float, float], NamedLocation] | None = None,
        altitudes: Mapping[tuple[float, float], float] | None = None,
    ) -> None:
        self._locations: dict[str, Location] = dict(locations or {})
        self._named_locations: dict[tuple[float, float], NamedLocation] = dict(named_locations or {})
        self._altitudes: dict[tuple[float, float], float] = dict(altitudes or {})

    def geocode(self, address: str) -> Location:
        """Resolve ``address`` from the static table."""
        if (location := self._locations.get(address)) is None:
            raise ValueError(f"Could not geocode address: {address!r}.")
        return location

    def reverse_geocode(self, location: Location, language: str | None = None) -> NamedLocation:
        """Resolve ``location`` coordinates from the static table.

        ``language`` is accepted for :class:`~hyperion.ports.geocoder.Geocoder`
        compatibility and ignored (static data is language-agnostic).
        """
        key = (location.latitude, location.longitude)
        if (named := self._named_locations.get(key)) is None:
            raise ValueError(f"Could not reverse-geocode provided location - no results. {location!r}")
        return named

    def get_altitude(self, location: Location) -> float:
        """Return the altitude of ``location`` from the static table."""
        key = (location.latitude, location.longitude)
        if (altitude := self._altitudes.get(key)) is None:
            raise ValueError(f"No elevation data found for location {location!r}.")
        return altitude
