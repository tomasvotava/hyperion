"""Port: geocoder abstraction.

``Geocoder`` is the dependency-inversion seam between callers that need
address <-> coordinate resolution and a concrete backend (Google Maps, or a
static offline lookup for tests). It is a ``runtime_checkable``
:class:`typing.Protocol` -- adapters satisfy it structurally, no forced
inheritance (see ``docs/ddd-refactor-plan.md`` F4 / Step 7).

The port references only the pure-domain :class:`~hyperion.domain.geo.Location`
and :class:`~hyperion.domain.geo.NamedLocation` value objects, so importing it
stays lite (no ``googlemaps``, no boto3).

Contract:

* ``geocode`` raises :class:`ValueError` when the address cannot be resolved.
* ``reverse_geocode`` raises :class:`ValueError` when the location has no
  address.
* ``get_altitude`` raises :class:`ValueError` when no elevation data exists.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from hyperion.domain.geo import Location, NamedLocation


@runtime_checkable
class Geocoder(Protocol):
    """Abstraction over geocoding backends."""

    def geocode(self, address: str) -> Location:
        """Resolve ``address`` to a :class:`Location`.

        Raises:
            ValueError: if the address cannot be geocoded.
        """
        ...

    def reverse_geocode(self, location: Location, language: str | None = None) -> NamedLocation:
        """Resolve ``location`` coordinates to a named address.

        Args:
            location: The coordinates to reverse-geocode.
            language: Optional language for the returned components.

        Raises:
            ValueError: if the location cannot be reverse-geocoded.
        """
        ...

    def get_altitude(self, location: Location) -> float:
        """Return the altitude of ``location`` in meters above sea level.

        Raises:
            ValueError: if no elevation data is available.
        """
        ...
