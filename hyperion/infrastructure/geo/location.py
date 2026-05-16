"""Common location classes and functions.

.. deprecated::
    The geographical primitives (:class:`Location`, :class:`NamedLocation`,
    :class:`SpatialKMeans`, :func:`meters_to_degrees`, :data:`EARTH_RADIUS_METERS`,
    :data:`LATITUDE_DEGREE_TO_METERS`) moved to :mod:`hyperion.domain.geo`
    (F2 / DDD refactor Step 3) and the ``Location._cache`` singleton was
    removed. Import them from :mod:`hyperion.domain.geo` instead. This module
    keeps them importable (with a :class:`DeprecationWarning`, resolved lazily
    so the import does not pull numpy) for the whole ``hyperion-sdk`` 1.x line.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr

if TYPE_CHECKING:
    from hyperion.domain.geo import (
        EARTH_RADIUS_METERS,
        LATITUDE_DEGREE_TO_METERS,
        Location,
        NamedLocation,
        SpatialKMeans,
        meters_to_degrees,
    )

_MOVED: dict[str, str] = {
    "LATITUDE_DEGREE_TO_METERS": "hyperion.domain.geo",
    "EARTH_RADIUS_METERS": "hyperion.domain.geo",
    "meters_to_degrees": "hyperion.domain.geo",
    "SpatialKMeans": "hyperion.domain.geo",
    "Location": "hyperion.domain.geo",
    "NamedLocation": "hyperion.domain.geo",
}

__all__ = [
    "EARTH_RADIUS_METERS",
    "LATITUDE_DEGREE_TO_METERS",
    "Location",
    "NamedLocation",
    "SpatialKMeans",
    "meters_to_degrees",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        new_module = _MOVED[name]
        # Deferred import: a deprecated geo import must not pull numpy -- only
        # `hyperion.domain.geo.SpatialKMeans` use does.
        module = importlib.import_module(new_module)
        return moved_attr(
            name=name,
            value=getattr(module, name),
            old_module="hyperion.infrastructure.geo.location",
            new_module=new_module,
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
