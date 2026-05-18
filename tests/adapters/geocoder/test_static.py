"""Tests for `hyperion.adapters.geocoder.static.StaticGeocoder` (lite adapter).

Substitutable behind the `Geocoder` port: its error messages mirror
`GoogleMaps` so callers can swap backends without behaviour change.
"""

import pytest

from hyperion.adapters.geocoder.static import StaticGeocoder
from hyperion.domain.geo import Location, NamedLocation


def test_geocode_known_address() -> None:
    paris = Location(latitude=48.8566, longitude=2.3522, title="Paris", address="Paris, France")
    geocoder = StaticGeocoder(locations={"Paris": paris})
    assert geocoder.geocode("Paris") is paris


def test_geocode_unknown_address_raises() -> None:
    geocoder = StaticGeocoder()
    with pytest.raises(ValueError, match="Could not geocode address"):
        geocoder.geocode("nowhere")


def test_reverse_geocode_known_location() -> None:
    loc = Location(latitude=1.0, longitude=2.0)
    named = NamedLocation(location=loc, route="Main St", country="Wonderland")
    geocoder = StaticGeocoder(named_locations={(1.0, 2.0): named})
    assert geocoder.reverse_geocode(loc) is named
    # language is accepted and ignored for port compatibility.
    assert geocoder.reverse_geocode(loc, language="de") is named


def test_reverse_geocode_unknown_location_raises() -> None:
    geocoder = StaticGeocoder()
    with pytest.raises(ValueError, match="Could not reverse-geocode"):
        geocoder.reverse_geocode(Location(0.0, 0.0))


def test_get_altitude_known_location() -> None:
    geocoder = StaticGeocoder(altitudes={(1.0, 2.0): 321.0})
    assert geocoder.get_altitude(Location(1.0, 2.0)) == pytest.approx(321.0)


def test_get_altitude_unknown_location_raises() -> None:
    geocoder = StaticGeocoder()
    with pytest.raises(ValueError, match="No elevation data"):
        geocoder.get_altitude(Location(9.9, 9.9))


def test_satisfies_geocoder_protocol() -> None:
    from hyperion.ports.geocoder import Geocoder

    assert isinstance(StaticGeocoder(), Geocoder)
