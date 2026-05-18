"""Deprecated import shim for cache adapters.

.. deprecated::
    The abstract :class:`Cache`, :class:`CacheStats` and :class:`CachingError`
    moved to :mod:`hyperion.ports.cache`. The concrete adapters moved to
    ``hyperion.adapters.cache.*`` (``InMemoryCache`` ->
    :mod:`hyperion.adapters.cache.memory`, ``LocalFileCache`` ->
    :mod:`hyperion.adapters.cache.filesystem`, ``DynamoDBCache`` ->
    :mod:`hyperion.adapters.cache.dynamodb`). :class:`PersistentCache` -- the
    ``Catalog`` <-> cache knot (F3 / Step 7) -- moved to
    :mod:`hyperion.application.persistent_cache` and is deprecated in favour of
    an injected :class:`~hyperion.ports.keyval.KeyValueStore`. Import them from
    their new locations. This module keeps every relocated symbol importable
    (with a :class:`DeprecationWarning`, resolved lazily so the import pulls
    neither boto3 nor ``Catalog`` / ``fastavro``) for the whole
    ``hyperion-sdk`` 1.x line; symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.cache import DEFAULT_TTL_SECONDS, CacheKeyOpenMode
from hyperion.ports.cache import Cache as _Cache
from hyperion.ports.cache import CacheStats as _CacheStats
from hyperion.ports.cache import CachingError as _CachingError

if TYPE_CHECKING:
    from hyperion.adapters.cache.dynamodb import DYNAMODB_MAX_LENGTH, DynamoDBCache
    from hyperion.adapters.cache.filesystem import DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE, LocalFileCache
    from hyperion.adapters.cache.memory import InMemoryCache
    from hyperion.application.persistent_cache import PersistentCache
    from hyperion.ports.cache import Cache, CacheStats, CachingError

_OLD_MODULE = "hyperion.infrastructure.cache"

# Abstract contract types -- already imported from the lite ``ports`` layer.
_MOVED: dict[str, tuple[object, str]] = {
    "Cache": (_Cache, "hyperion.ports.cache"),
    "CacheStats": (_CacheStats, "hyperion.ports.cache"),
    "CachingError": (_CachingError, "hyperion.ports.cache"),
}

# Concrete adapters + their constants -- resolved lazily so importing this
# shim never pulls boto3 (the DynamoDB adapter). ``PersistentCache`` is resolved
# lazily too, so importing this shim no longer drags ``Catalog`` / ``fastavro``.
_MOVED_LAZY: dict[str, str] = {
    "InMemoryCache": "hyperion.adapters.cache.memory",
    "LocalFileCache": "hyperion.adapters.cache.filesystem",
    "DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE": "hyperion.adapters.cache.filesystem",
    "DynamoDBCache": "hyperion.adapters.cache.dynamodb",
    "DYNAMODB_MAX_LENGTH": "hyperion.adapters.cache.dynamodb",
    "PersistentCache": "hyperion.application.persistent_cache",
}

__all__ = [
    "DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE",
    "DEFAULT_TTL_SECONDS",
    "DYNAMODB_MAX_LENGTH",
    "Cache",
    "CacheKeyOpenMode",
    "CacheStats",
    "CachingError",
    "DynamoDBCache",
    "InMemoryCache",
    "LocalFileCache",
    "PersistentCache",
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
