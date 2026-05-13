"""Tests for `hyperion.infrastructure.secretsmanager`.

Lock the env-translation pattern, the Dummy/AWS implementations, and the
`from_config()` dispatch table before the refactor moves these to
`adapters/secrets/{env,dummy,aws_sm}.py`.
"""

import os
from collections.abc import Iterator

import boto3
import pytest

from hyperion.infrastructure.secretsmanager import (
    AWSSecretsManager,
    DummySecretsManager,
    SecretsManager,
)

BACKEND_ENV = "HYPERION_SECRETS_BACKEND"


@pytest.fixture(autouse=True)
def _reset_singleton() -> Iterator[None]:
    # SecretsManager._instance is a class-level cache. Wipe it before / after each
    # test so dispatch tests see a clean factory.
    previous = SecretsManager._instance
    SecretsManager._instance = None
    yield
    SecretsManager._instance = previous


@pytest.fixture(autouse=True)
def _clean_backend_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # SecretsConfig fields are not allow_set; control via env var.
    monkeypatch.delenv(BACKEND_ENV, raising=False)


class TestTranslateEnvVars:
    def test_replaces_pattern(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No backend set → dispatch resolves to Dummy; secrets resolve to "".
        monkeypatch.setenv("SECRET_DB_PASSWORD", "!#secret:#db_password")
        monkeypatch.setenv("UNRELATED", "hello")

        SecretsManager.translate_env_vars()

        assert os.environ["SECRET_DB_PASSWORD"] == ""
        assert os.environ["UNRELATED"] == "hello"

    def test_leaves_non_matching_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOOKS_SIMILAR", "secret:#db_password")  # missing '!#'
        monkeypatch.setenv("NORMAL", "value")

        SecretsManager.translate_env_vars()

        assert os.environ["LOOKS_SIMILAR"] == "secret:#db_password"
        assert os.environ["NORMAL"] == "value"


class TestDummySecretsManager:
    def test_get_secret_returns_empty_string(self) -> None:
        manager = DummySecretsManager()
        assert manager.get_secret("anything") == ""


class TestFromConfig:
    def test_no_backend_returns_dummy(self) -> None:
        instance = SecretsManager.from_config()
        assert isinstance(instance, DummySecretsManager)

    def test_singleton_cached(self) -> None:
        first = SecretsManager.from_config()
        second = SecretsManager.from_config()
        assert first is second

    def test_unknown_backend_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(BACKEND_ENV, "VaultThing")
        with pytest.raises(ValueError, match="Unsupported secrets backend"):
            SecretsManager.from_config()


@pytest.mark.usefixtures("_moto_server")
class TestAWSSecretsManager:
    def test_aws_backend_dispatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(BACKEND_ENV, "AWSSecretsManager")
        instance = SecretsManager.from_config()
        assert isinstance(instance, AWSSecretsManager)

    def test_get_secret_roundtrip(self) -> None:
        client = boto3.client("secretsmanager")
        client.create_secret(Name="my/api-key", SecretString="super-secret-value")  # pragma: allowlist secret
        manager = AWSSecretsManager()
        assert manager.get_secret("my/api-key") == "super-secret-value"  # pragma: allowlist secret
