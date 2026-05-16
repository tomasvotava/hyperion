"""Deprecated import shim for secrets manager adapters.

.. deprecated::
    The abstract :class:`SecretsManager` and :data:`SECRET_PATTERN` moved to
    :mod:`hyperion.ports.secrets`. The concrete adapters moved to
    ``hyperion.adapters.secrets.*`` (``DummySecretsManager`` ->
    :mod:`hyperion.adapters.secrets.dummy`, ``AWSSecretsManager`` ->
    :mod:`hyperion.adapters.secrets.aws_sm`). Import them from there. This
    module keeps every symbol importable (with a :class:`DeprecationWarning`,
    resolved lazily so the import does not pull boto3) for the whole
    ``hyperion-sdk`` 1.x line. The symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.secrets import SECRET_PATTERN as _SECRET_PATTERN
from hyperion.ports.secrets import SecretsManager as _SecretsManager

if TYPE_CHECKING:
    from hyperion.adapters.secrets.aws_sm import AWSSecretsManager
    from hyperion.adapters.secrets.dummy import DummySecretsManager
    from hyperion.ports.secrets import SECRET_PATTERN, SecretsManager

_OLD_MODULE = "hyperion.infrastructure.secretsmanager"

_MOVED: dict[str, tuple[object, str]] = {
    "SecretsManager": (_SecretsManager, "hyperion.ports.secrets"),
    "SECRET_PATTERN": (_SECRET_PATTERN, "hyperion.ports.secrets"),
}

_MOVED_LAZY: dict[str, str] = {
    "DummySecretsManager": "hyperion.adapters.secrets.dummy",  # pragma: allowlist secret
    "AWSSecretsManager": "hyperion.adapters.secrets.aws_sm",  # pragma: allowlist secret
}

__all__ = [
    "SECRET_PATTERN",
    "AWSSecretsManager",
    "DummySecretsManager",
    "SecretsManager",
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
