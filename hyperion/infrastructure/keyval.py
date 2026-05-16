"""Deprecated import shim for key-value store adapters.

.. deprecated::
    The abstract :class:`KeyValueStore`, the :data:`CompressionType` alias and
    the :func:`is_valid_compression_type` guard moved to
    :mod:`hyperion.ports.keyval`. The concrete adapters moved to
    ``hyperion.adapters.keyval.*`` (``InMemoryStore`` ->
    :mod:`hyperion.adapters.keyval.memory`, ``DynamoDBStore`` ->
    :mod:`hyperion.adapters.keyval.dynamodb`). Import them from there. This
    module keeps every symbol importable (with a :class:`DeprecationWarning`,
    resolved lazily so the import does not pull boto3) for the whole
    ``hyperion-sdk`` 1.x line. The symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.keyval import CompressionType as _CompressionType
from hyperion.ports.keyval import KeyValueStore as _KeyValueStore
from hyperion.ports.keyval import is_valid_compression_type as _is_valid_compression_type

if TYPE_CHECKING:
    from hyperion.adapters.keyval.dynamodb import DynamoDBStore
    from hyperion.adapters.keyval.memory import InMemoryStore
    from hyperion.ports.keyval import CompressionType, KeyValueStore, is_valid_compression_type

_OLD_MODULE = "hyperion.infrastructure.keyval"

# Symbols already imported here from the lite ``ports`` layer -- returned eagerly.
_MOVED: dict[str, tuple[object, str]] = {
    "KeyValueStore": (_KeyValueStore, "hyperion.ports.keyval"),
    "CompressionType": (_CompressionType, "hyperion.ports.keyval"),
    "is_valid_compression_type": (_is_valid_compression_type, "hyperion.ports.keyval"),
}

# Concrete adapters -- resolved lazily so importing this shim stays boto3-free.
_MOVED_LAZY: dict[str, str] = {
    "InMemoryStore": "hyperion.adapters.keyval.memory",
    "DynamoDBStore": "hyperion.adapters.keyval.dynamodb",
}

__all__ = [
    "CompressionType",
    "DynamoDBStore",
    "InMemoryStore",
    "KeyValueStore",
    "is_valid_compression_type",
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
