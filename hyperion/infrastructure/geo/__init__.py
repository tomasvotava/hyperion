"""Deprecated geo namespace.

``Location`` is re-exported from the pure-domain :mod:`hyperion.domain.geo`
(lite). ``GoogleMaps`` moved to :mod:`hyperion.adapters.geocoder.google`; it is
resolved lazily here (with a :class:`DeprecationWarning`) so touching this
namespace does not pull ``googlemaps``. Removed in ``hyperion-sdk`` 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.domain.geo import Location

if TYPE_CHECKING:
    from hyperion.adapters.geocoder.google import GoogleMaps

_OLD_MODULE = "hyperion.infrastructure.geo"

_MOVED_LAZY: dict[str, str] = {
    "GoogleMaps": "hyperion.adapters.geocoder.google",
}

__all__ = [
    "GoogleMaps",
    "Location",
]


def __getattr__(name: str) -> object:
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
