"""Google Maps API client."""

import json
from contextlib import ExitStack
from dataclasses import asdict, replace
from typing import Any, ClassVar

import googlemaps

from hyperion.config import geo_config
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.infrastructure.cache import PersistentCache
from hyperion.infrastructure.geo.location import Location, NamedLocation
from hyperion.logging import get_logger

logger = get_logger("gmaps")

cache_asset = PersistentStoreAsset("GEOCodeCache", schema_version=1)


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
    """Google Maps API client."""

    _instance: ClassVar["GoogleMaps | None"] = None

    @classmethod
    def from_config(cls) -> "GoogleMaps":
        """Get the Google Maps API client instance from the configuration."""
        if cls._instance is None:
            if geo_config.gmaps_api_key is None:
                raise ValueError("Google Maps API key is not set.")
            cls._instance = GoogleMaps(api_key=geo_config.gmaps_api_key)
        return cls._instance

    def __init__(self, api_key: str) -> None:
        """Initialize the Google Maps API client.

        Args:
            api_key (str): The Google Maps API key.
        """
        self.geocode_cache = PersistentCache("gmaps", hash_keys=False, asset=cache_asset)
        self.client = googlemaps.Client(key=api_key)
        self._cache_context: ExitStack | None = None

    def __enter__(self) -> None:
        self._cache_context = ExitStack()
        self._cache_context.enter_context(self.geocode_cache)

    def __exit__(self, *args: Any) -> None:
        if self._cache_context is None:
            return
        self._cache_context.close()

    def geocode(self, address: str) -> Location:
        """Geocode an address.

        Args:
            address (str): The address to geocode.

        Returns:
            Location: The geocoded location.
        """
        if (cached_location := self.geocode_cache.get(address)) is not None:
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
        self.geocode_cache.set(address, json.dumps(asdict(location)))
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
