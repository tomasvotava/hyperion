import abc
import os
import re
from typing import cast

import boto3

from hyperion.config import secrets_config
from hyperion.logging import get_logger

logger = get_logger("hyperion-secrets")

SECRET_PATTERN = re.compile(r"!#secret:#(?P<secret_name>.+)")


class SecretsManager(abc.ABC):
    _instance: "SecretsManager" | None = None

    @staticmethod
    def _create_new() -> "SecretsManager":
        if secrets_config.backend is None:
            logger.warning("No secrets backend is configured. Using dummy secrets manager.")
            return DummySecretsManager()
        if secrets_config.backend == "AWSSecretsManager":
            logger.info("Using AWS Secrets Manager.")
            return AWSSecretsManager()
        raise ValueError(f"Unsupported secrets backend: {secrets_config.backend!r}.")

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


class DummySecretsManager(SecretsManager):
    def get_secret(self, secret_name: str) -> str:
        logger.warning("Using dummy secrets manager, no values will be returned.", secret_name=secret_name)
        return ""


class AWSSecretsManager(SecretsManager):
    def __init__(self) -> None:
        self.client = boto3.client("secretsmanager")

    def get_secret(self, secret_name: str) -> str:
        return cast(str, self.client.get_secret_value(SecretId=secret_name)["SecretString"])
