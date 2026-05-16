"""AWS Secrets Manager :class:`SecretsManager` adapter (requires boto3 -- ``[aws]``)."""

from __future__ import annotations

from typing import cast

import boto3

from hyperion.ports.secrets import SecretsManager


class AWSSecretsManager(SecretsManager):
    def __init__(self) -> None:
        self.client = boto3.client("secretsmanager")

    def get_secret(self, secret_name: str) -> str:
        return cast(str, self.client.get_secret_value(SecretId=secret_name)["SecretString"])
