"""Deprecated import shim for schema store adapters.

.. deprecated::
    The abstract :class:`SchemaStore` moved to
    :mod:`hyperion.ports.schema_registry`. The concrete adapters moved to
    ``hyperion.adapters.schema_registry.*`` (``LocalSchemaStore`` /
    :data:`AVRO_SCHEMAS_PATH` -> :mod:`hyperion.adapters.schema_registry.local`,
    ``S3SchemaStore`` -> :mod:`hyperion.adapters.schema_registry.s3`). Import
    them from there. This module keeps every symbol importable (with a
    :class:`DeprecationWarning`, resolved lazily so the import does not pull
    boto3) for the whole ``hyperion-sdk`` 1.x line. The symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.schema_registry import SchemaStore as _SchemaStore

if TYPE_CHECKING:
    from hyperion.adapters.schema_registry.local import AVRO_SCHEMAS_PATH, LocalSchemaStore
    from hyperion.adapters.schema_registry.s3 import S3SchemaStore
    from hyperion.ports.schema_registry import SchemaStore

_OLD_MODULE = "hyperion.catalog.schema"

_MOVED: dict[str, tuple[object, str]] = {
    "SchemaStore": (_SchemaStore, "hyperion.ports.schema_registry"),
}

_MOVED_LAZY: dict[str, str] = {
    "AVRO_SCHEMAS_PATH": "hyperion.adapters.schema_registry.local",
    "LocalSchemaStore": "hyperion.adapters.schema_registry.local",
    "S3SchemaStore": "hyperion.adapters.schema_registry.s3",
}

__all__ = [
    "AVRO_SCHEMAS_PATH",
    "LocalSchemaStore",
    "S3SchemaStore",
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
