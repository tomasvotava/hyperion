"""Unit tests for the new lite ``EnvSecretsManager`` adapter.

Resolves secrets from process environment variables. Not wired into
``SecretsManager.from_config`` until S8 -- tested directly here.
"""

import pytest

from hyperion.adapters.secrets.env import EnvSecretsManager
from hyperion.ports.secrets import SecretsManager


def test_is_a_secrets_manager() -> None:
    assert isinstance(EnvSecretsManager(), SecretsManager)


def test_returns_environment_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_SECRET", "s3cr3t")  # pragma: allowlist secret
    assert EnvSecretsManager().get_secret("MY_SECRET") == "s3cr3t"


def test_raises_keyerror_for_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ABSENT_SECRET", raising=False)
    with pytest.raises(KeyError):
        EnvSecretsManager().get_secret("ABSENT_SECRET")
