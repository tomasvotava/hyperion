"""Environment-variable :class:`SecretsManager` adapter (lite -- ``os.environ``)."""

from __future__ import annotations

import os

from hyperion.log import get_logger
from hyperion.ports.secrets import SecretsManager

logger = get_logger("hyperion-secrets")


class EnvSecretsManager(SecretsManager):
    """Resolve secrets from process environment variables.

    The secret name is looked up verbatim as an environment variable.
    """

    def get_secret(self, secret_name: str) -> str:
        try:
            return os.environ[secret_name]
        except KeyError:
            logger.error("Secret not found in environment.", secret_name=secret_name)
            raise
