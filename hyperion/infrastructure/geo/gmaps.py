"""Deprecated import shim for the Google Maps geocoder.

.. deprecated::
    ``GoogleMaps`` moved to :mod:`hyperion.adapters.geocoder.google` and now
    implements the :class:`~hyperion.ports.geocoder.Geocoder` port with an
    injected :class:`~hyperion.ports.keyval.KeyValueStore` (no more
    ``PersistentCache`` / ``Catalog`` knot, F4 / Step 7). Import it from there.
    This module keeps the symbol importable (with a :class:`DeprecationWarning`,
    resolved lazily so the import does not pull ``googlemaps``) for the whole
    ``hyperion-sdk`` 1.x line; it is removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr

if TYPE_CHECKING:
    from hyperion.adapters.geocoder.google import GoogleMaps

_OLD_MODULE = "hyperion.infrastructure.geo.gmaps"

# Resolved lazily so importing this shim never pulls ``googlemaps``.
_MOVED_LAZY: dict[str, str] = {
    "GoogleMaps": "hyperion.adapters.geocoder.google",
}

__all__ = [
    "GoogleMaps",
]


def __getattr__(name: str) -> object:
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
