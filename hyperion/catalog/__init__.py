"""Deprecated import shim for the ``hyperion.catalog`` namespace.

.. deprecated::
    Touching this namespace must not eagerly load ``fastavro``/boto3 (F8 /
    Step 9). Import the symbols from their explicit modules instead:
    :class:`Catalog` / :class:`AssetNotFoundError` ->
    :mod:`hyperion.catalog.catalog`, the abstract :class:`SchemaStore` ->
    :mod:`hyperion.ports.schema_registry`. This package keeps every symbol
    importable (with a :class:`DeprecationWarning`, ``Catalog`` /
    ``AssetNotFoundError`` resolved lazily so ``import hyperion.catalog`` pulls
    neither ``fastavro`` nor boto3) for the whole ``hyperion-sdk`` 1.x line. The
    aliases are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.schema_registry import SchemaStore as _SchemaStore

if TYPE_CHECKING:
    from hyperion.catalog.catalog import AssetNotFoundError, Catalog
    from hyperion.ports.schema_registry import SchemaStore

_OLD_MODULE = "hyperion.catalog"

# Abstract contract type -- already imported from the lite ``ports`` layer.
_MOVED: dict[str, tuple[object, str]] = {
    "SchemaStore": (_SchemaStore, "hyperion.ports.schema_registry"),
}

# ``Catalog`` + ``AssetNotFoundError`` resolved lazily so importing this
# namespace never pulls ``fastavro`` (the avro serializer) or boto3.
_MOVED_LAZY: dict[str, str] = {
    "Catalog": "hyperion.catalog.catalog",
    "AssetNotFoundError": "hyperion.catalog.catalog",
}

__all__ = [
    "AssetNotFoundError",
    "Catalog",
    "SchemaStore",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        value, new_module = _MOVED[name]
        return moved_attr(name=name, value=value, old_module=_OLD_MODULE, new_module=new_module)
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
