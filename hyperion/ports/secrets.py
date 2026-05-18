"""Port: secrets manager abstraction.

Abstract :class:`SecretsManager` base and the :data:`SECRET_PATTERN` used by
``translate_env_vars``. Concrete adapters (``DummySecretsManager``,
``AWSSecretsManager``, ``EnvSecretsManager``) live in
``hyperion.adapters.secrets.*``; ``_create_new`` delegates backend selection to
:mod:`hyperion.composition` (the single composition root).
"""

import abc
import os
import re

from hyperion.log import get_logger

logger = get_logger("hyperion-secrets")

SECRET_PATTERN = re.compile(r"!#secret:#(?P<secret_name>.+)")


class SecretsManager(abc.ABC):
    _instance: "SecretsManager | None" = None

    @staticmethod
    def _create_new() -> "SecretsManager":
        from hyperion import composition

        return composition.default_secrets()

    @staticmethod
    def from_config() -> "SecretsManager":
        if SecretsManager._instance is None:
            SecretsManager._instance = SecretsManager._create_new()
        return SecretsManager._instance

    @staticmethod
    def translate_env_vars() -> None:
        """Loop through all env variables and replace secrets with their values.

        Only variables with value pattern of `!#secret:#secret_name` will be replaced.
        """
        for key, value in os.environ.items():
            if (match := SECRET_PATTERN.match(value)) is not None:
                secret_name = match.group("secret_name")
                logger.info("Replacing secret in environment variable.", key=key, secret_name=secret_name)
                os.environ[key] = SecretsManager.from_config().get_secret(secret_name)

    @abc.abstractmethod
    def get_secret(self, secret_name: str) -> str:
        """Get the secret with the given name."""
