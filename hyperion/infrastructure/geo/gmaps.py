"""Google Maps API client."""

import json
from contextlib import ExitStack
from dataclasses import asdict
from typing import Any, ClassVar

import googlemaps

from hyperion.config import geo_config
from hyperion.entities.catalog import PersistentStoreAsset
from hyperion.infrastructure.cache import PersistentCache
from hyperion.infrastructure.geo.location import Location
from hyperion.logging import get_logger

logger = get_logger("gmaps")

cache_asset = PersistentStoreAsset("GEOCodeCache", schema_version=1)


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
