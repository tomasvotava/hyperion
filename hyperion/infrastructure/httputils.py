"""Deprecated import shim for HTTP proxy helpers.

.. deprecated::
    :func:`redact_url` and :func:`get_proxy_mounts` moved to
    :mod:`hyperion.adapters.http.proxy`. Import them from there. This module
    keeps them importable (with a :class:`DeprecationWarning`, resolved lazily)
    for the whole ``hyperion-sdk`` 1.x line. The symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr

if TYPE_CHECKING:
    from hyperion.adapters.http.proxy import get_proxy_mounts, redact_url

_OLD_MODULE = "hyperion.infrastructure.httputils"

_MOVED_LAZY: dict[str, str] = {
    "redact_url": "hyperion.adapters.http.proxy",
    "get_proxy_mounts": "hyperion.adapters.http.proxy",
}

__all__ = [
    "get_proxy_mounts",
    "redact_url",
]


def __getattr__(name: str) -> object:
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
