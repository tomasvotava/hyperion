"""Google Maps :class:`~hyperion.ports.geocoder.Geocoder` adapter.

Requires the ``googlemaps`` client (``[geo]`` extra). The geocode cache is an
injected :class:`~hyperion.ports.keyval.KeyValueStore` -- this adapter never
touches :class:`Catalog` (the ``PersistentCache`` knot removed in Step 7,
``docs/ddd-refactor-plan.md`` F3 / F4).
"""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from typing import Any

import googlemaps

from hyperion.adapters.keyval.memory import InMemoryStore
from hyperion.config import geo_config
from hyperion.domain.geo import Location, NamedLocation
from hyperion.log import get_logger
from hyperion.ports.keyval import KeyValueStore

logger = get_logger("gmaps")


def _find_info_by_type(components: list[dict[str, Any]], info_type: str) -> str | None:
    for component in components:
        if not isinstance((component_types := component.get("types")), list):
            raise TypeError(f"Unexpected component type info, expected 'list', got {type(component_types)!r}.")
        for component_type in component_types:
            if component_type == info_type:
                value = component.get("long_name", component.get("short_name"))
                if value is None or isinstance(value, str):
                    return value
                raise TypeError(f"Unexpected value type, expected 'str' or None, got {type(value)!r}.")
    return None


class GoogleMaps:
    """Google Maps API client implementing :class:`~hyperion.ports.geocoder.Geocoder`."""

    @classmethod
    def from_config(cls) -> GoogleMaps:
        """Build a Google Maps client from the configuration.

        Returns a fresh instance (no singleton); the geocode cache defaults to
        a non-persistent :class:`InMemoryStore`. Wire a persistent
        :class:`~hyperion.ports.keyval.KeyValueStore` explicitly if you need
        cross-process caching.
        """
        if geo_config.gmaps_api_key is None:
            raise ValueError("Google Maps API key is not set.")
        return cls(api_key=geo_config.gmaps_api_key)

    def __init__(self, api_key: str, keyval: KeyValueStore | None = None) -> None:
        """Initialize the Google Maps API client.

        Args:
            api_key (str): The Google Maps API key.
            keyval (KeyValueStore, optional): Backing store for the geocode
                cache. Defaults to a non-persistent :class:`InMemoryStore`.
        """
        self.keyval: KeyValueStore = keyval if keyval is not None else InMemoryStore()
        self.client = googlemaps.Client(key=api_key)

    def __enter__(self) -> GoogleMaps:
        """No-op; retained so ``with GoogleMaps(...) as g:`` keeps working."""
        return self

    def __exit__(self, *args: Any) -> None:
        """No-op; the geocode cache persists per-key via the injected store."""
        return None

    def geocode(self, address: str) -> Location:
        """Geocode an address.

        Args:
            address (str): The address to geocode.

        Returns:
            Location: The geocoded location.
        """
        if (cached_location := self.keyval.get(address)) is not None:
            logger.debug("Using geocoded information from cache.", address=address, location=cached_location)
            return Location(**json.loads(cached_location))
        result = self.client.geocode(address)
        if not result:
            raise ValueError(f"Could not geocode address: {address!r}.")
        location = Location(
            latitude=result[0]["geometry"]["location"]["lat"],
            longitude=result[0]["geometry"]["location"]["lng"],
            title=address,
            address=result[0]["formatted_address"],
        )
        logger.debug("Found geocoded information on address.", address=address, location=location)
        self.keyval.set(address, json.dumps(asdict(location)))
        return location

    def reverse_geocode(self, location: Location, language: str | None = None) -> NamedLocation:
        """Reverse geocode a location into an address.

        Args:
            location (Location): The location coordinates.
            language (str, optional): The language in which to return results. Defaults
                to None.

        Returns:
            str: The address name.
        """
        results = self.client.reverse_geocode({"lat": location.latitude, "lng": location.longitude}, language=language)
        if not results or not isinstance(results, list):
            raise ValueError(f"Could not reverse-geocode provided location - no results. {location!r}")
        result = results[0]
        if not isinstance(result, dict):
            raise TypeError(f"Unexpected result type, expected 'dict', got {type(result)!r}.")
        if not isinstance(address_components := result.get("address_components"), list):
            raise TypeError(f"Unexpected address components type, expected 'dict', got {type(address_components)!r}.")
        title = _find_info_by_type(address_components, "route")
        return NamedLocation(
            location=replace(
                location, address=result.get("formatted_address") or location.address, title=title or location.title
            ),
            route=title,
            neighborhood=_find_info_by_type(address_components, "neighborhood"),
            sublocality=_find_info_by_type(address_components, "sublocality")
            or _find_info_by_type(address_components, "sublocality_level_1"),
            administrative_area=_find_info_by_type(address_components, "administrative_area_level_1"),
            administrative_area_level_2=_find_info_by_type(address_components, "administrative_area_level_2"),
            country=_find_info_by_type(address_components, "country"),
            address=result.get("formatted_address"),
        )

    def get_altitude(self, location: Location) -> float:
        """Get altitude of the given location using Elevation API.

        Args:
            location (Location): The location coordinates.

        Returns:
            float: The altitude in meters above sea level
        """
        result = self.client.elevation((location.latitude, location.longitude))
        if not result:
            raise ValueError(f"No elevation data found for location {location!r}.")
        if not isinstance(result[0], dict) or "elevation" not in result[0]:
            raise ValueError("Unexpected data returned by the Google Maps Elevation API.")
        return float(result[0]["elevation"])
