"""Concrete secrets manager adapters.

.. deprecated::
    The abstract :class:`SecretsManager` and :data:`SECRET_PATTERN` moved to
    :mod:`hyperion.ports.secrets`. Import them from there. This module keeps
    them importable (with a :class:`DeprecationWarning`) for the whole
    ``hyperion-sdk`` 1.x line and still hosts the concrete adapters until S6.
"""

from typing import TYPE_CHECKING, cast

import boto3

from hyperion._compat import moved_attr
from hyperion.log import get_logger
from hyperion.ports.secrets import SECRET_PATTERN as _SECRET_PATTERN
from hyperion.ports.secrets import SecretsManager as _SecretsManager

if TYPE_CHECKING:
    from hyperion.ports.secrets import SECRET_PATTERN, SecretsManager

logger = get_logger("hyperion-secrets")

_MOVED: dict[str, tuple[object, str]] = {
    "SecretsManager": (_SecretsManager, "hyperion.ports.secrets"),
    "SECRET_PATTERN": (_SECRET_PATTERN, "hyperion.ports.secrets"),
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
        return moved_attr(
            name=name, value=value, old_module="hyperion.infrastructure.secretsmanager", new_module=new_module
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class DummySecretsManager(_SecretsManager):
    def get_secret(self, secret_name: str) -> str:
        logger.warning("Using dummy secrets manager, no values will be returned.", secret_name=secret_name)
        return ""


class AWSSecretsManager(_SecretsManager):
    def __init__(self) -> None:
        self.client = boto3.client("secretsmanager")

    def get_secret(self, secret_name: str) -> str:
        return cast(str, self.client.get_secret_value(SecretId=secret_name)["SecretString"])
