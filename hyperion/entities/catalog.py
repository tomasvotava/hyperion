"""Catalog asset models.

.. deprecated::
    The asset identities (:data:`AssetType`, :class:`AssetProtocol`,
    :func:`get_prefixed_path`, :class:`DataLakeAsset`,
    :class:`PersistentStoreAsset`, :class:`FeatureAsset`) moved to
    :mod:`hyperion.domain.assets`, and the feature models
    (:class:`FeatureModel`, :class:`PolarsFeatureModel`) moved to
    :mod:`hyperion.data.asset_schemas` (F6 / DDD refactor Step 2). Import them
    from there. This module keeps them importable (with a
    :class:`DeprecationWarning`, resolved lazily so the import does not pull the
    data stack) for the whole ``hyperion-sdk`` 1.x line.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr

if TYPE_CHECKING:
    from hyperion.data.asset_schemas import FeatureModel, PolarsFeatureModel
    from hyperion.domain.assets import (
        AssetProtocol,
        AssetType,
        DataLakeAsset,
        FeatureAsset,
        PersistentStoreAsset,
        get_prefixed_path,
    )

_MOVED: dict[str, str] = {
    "AssetType": "hyperion.domain.assets",
    "AssetProtocol": "hyperion.domain.assets",
    "get_prefixed_path": "hyperion.domain.assets",
    "DataLakeAsset": "hyperion.domain.assets",
    "PersistentStoreAsset": "hyperion.domain.assets",
    "FeatureAsset": "hyperion.domain.assets",
    "FeatureModel": "hyperion.data.asset_schemas",
    "PolarsFeatureModel": "hyperion.data.asset_schemas",
}

__all__ = [
    "AssetProtocol",
    "AssetType",
    "DataLakeAsset",
    "FeatureAsset",
    "FeatureModel",
    "PersistentStoreAsset",
    "PolarsFeatureModel",
    "get_prefixed_path",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        new_module = _MOVED[name]
        # Deferred import: a deprecated *identity* import must not pull the
        # pandera/polars data stack -- only `hyperion.data.asset_schemas`
        # access does.
        module = importlib.import_module(new_module)
        return moved_attr(
            name=name, value=getattr(module, name), old_module="hyperion.entities.catalog", new_module=new_module
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
