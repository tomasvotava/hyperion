"""No-op :class:`SecretsManager` adapter (lite -- returns empty strings)."""

from __future__ import annotations

from hyperion.log import get_logger
from hyperion.ports.secrets import SecretsManager

logger = get_logger("hyperion-secrets")


class DummySecretsManager(SecretsManager):
    def get_secret(self, secret_name: str) -> str:
        logger.warning("Using dummy secrets manager, no values will be returned.", secret_name=secret_name)
        return ""
