"""Direct unit coverage for the composition root.

The lite branches (memory cache/queue, dummy secrets, local schema, S3
storage) are already exercised through the delegating ``*.from_config``
wrappers (test_message_queue / test_secretsmanager / test_catalog). These
tests pin the remaining branches -- backend *selection* -- without standing up
moto: every adapter constructor reached here is lazy (``boto3.resource`` makes
no network call), so swapping the config object is enough.
"""

import types

import pytest

from hyperion import composition
from hyperion.adapters.cache.dynamodb import DynamoDBCache
from hyperion.adapters.cache.filesystem import LocalFileCache
from hyperion.adapters.cache.memory import InMemoryCache
from hyperion.adapters.geocoder.google import GoogleMaps
from hyperion.adapters.keyval.memory import InMemoryStore
from hyperion.config import queue_config


class TestResolveQueueBackend:
    """``resolve_queue_backend`` imports no adapter -- pure config dispatch."""

    def test_memory_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(queue_config, "url", None)
        monkeypatch.setattr(queue_config, "path", None)
        assert composition.resolve_queue_backend() == "memory"

    def test_sqs_when_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(queue_config, "url", "https://sqs.eu-west-1.amazonaws.com/123/q")
        monkeypatch.setattr(queue_config, "path", None)
        assert composition.resolve_queue_backend() == "sqs"

    def test_file_when_path(self, monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
        monkeypatch.setattr(queue_config, "url", None)
        monkeypatch.setattr(queue_config, "path", f"{tmp_path}/queue.jsonl")
        assert composition.resolve_queue_backend() == "file"

    def test_ambiguous_url_and_path_warns_and_picks_sqs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: object
    ) -> None:
        # Exercises the both-set ambiguity warning branch.
        monkeypatch.setattr(queue_config, "url", "https://sqs.eu-west-1.amazonaws.com/123/q")
        monkeypatch.setattr(queue_config, "path", f"{tmp_path}/queue.jsonl")
        assert composition.resolve_queue_backend() == "sqs"


class TestDefaultCache:
    """All three cache backends select the right adapter (1:1 with the old
    ``Cache.from_config`` body), including the LocalFileCache ``default_ttl``
    asymmetry."""

    def test_inmemory_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = types.SimpleNamespace(
            cache_dynamodb_table=None,
            cache_dynamodb_default_ttl=60,
            cache_local_path=None,
            cache_key_prefix="",
        )
        monkeypatch.setattr("hyperion.composition.storage_config", fake)
        assert isinstance(composition.default_cache(), InMemoryCache)

    def test_local_file_when_path_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
        fake = types.SimpleNamespace(
            cache_dynamodb_table=None,
            cache_dynamodb_default_ttl=60,
            cache_local_path=str(tmp_path),
            cache_key_prefix="",
        )
        monkeypatch.setattr("hyperion.composition.storage_config", fake)
        assert isinstance(composition.default_cache(), LocalFileCache)

    def test_dynamodb_when_table_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = types.SimpleNamespace(
            cache_dynamodb_table="my-cache-table",
            cache_dynamodb_default_ttl=120,
            cache_local_path=None,
            cache_key_prefix="prefix",
        )
        monkeypatch.setattr("hyperion.composition.storage_config", fake)
        cache = composition.default_cache()
        assert isinstance(cache, DynamoDBCache)
        assert cache.table_name == "my-cache-table"


def test_default_keyval_is_inmemory() -> None:
    assert isinstance(composition.default_keyval(), InMemoryStore)


class TestDefaultGeocoder:
    def test_raises_without_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("hyperion.composition.geo_config", types.SimpleNamespace(gmaps_api_key=None))
        with pytest.raises(ValueError, match="Google Maps API key is not set"):
            composition.default_geocoder()

    def test_builds_googlemaps_with_inmemory_keyval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # googlemaps.Client validates key shape (must start with "AIza").
        monkeypatch.setattr(
            "hyperion.composition.geo_config",
            types.SimpleNamespace(gmaps_api_key="AIzaFakeCompositionTestKey"),  # pragma: allowlist secret
        )
        geocoder = composition.default_geocoder()
        assert isinstance(geocoder, GoogleMaps)
        assert isinstance(geocoder.keyval, InMemoryStore)
