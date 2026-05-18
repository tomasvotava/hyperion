"""S6 + S7 backward-compatibility contract.

Every symbol relocated by the ports/adapters split (F7 / Step 6) and the
geocoder/PersistentCache inversion (F3 / F4 / Step 7) must stay importable from
its old path for the whole ``hyperion-sdk`` 1.x line, emit a
:class:`DeprecationWarning` pointing at the new location, and resolve to the
*same* object as the new path. ``PersistentCache`` is now relocated (S7) to
:mod:`hyperion.application.persistent_cache` and deprecated in favour of an
injected ``KeyValueStore``; it is functional through 1.x and removed in 2.0.
"""

import importlib

import pytest

# (old_module, attribute, new_module)
RELOCATIONS = [
    # cache: abstracts -> ports, concretes + constants -> adapters
    ("hyperion.infrastructure.cache", "Cache", "hyperion.ports.cache"),
    ("hyperion.infrastructure.cache", "CacheStats", "hyperion.ports.cache"),
    ("hyperion.infrastructure.cache", "CachingError", "hyperion.ports.cache"),
    ("hyperion.infrastructure.cache", "InMemoryCache", "hyperion.adapters.cache.memory"),
    ("hyperion.infrastructure.cache", "LocalFileCache", "hyperion.adapters.cache.filesystem"),
    (
        "hyperion.infrastructure.cache",
        "DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE",
        "hyperion.adapters.cache.filesystem",
    ),
    ("hyperion.infrastructure.cache", "DynamoDBCache", "hyperion.adapters.cache.dynamodb"),
    ("hyperion.infrastructure.cache", "DYNAMODB_MAX_LENGTH", "hyperion.adapters.cache.dynamodb"),
    # keyval
    ("hyperion.infrastructure.keyval", "KeyValueStore", "hyperion.ports.keyval"),
    ("hyperion.infrastructure.keyval", "CompressionType", "hyperion.ports.keyval"),
    ("hyperion.infrastructure.keyval", "is_valid_compression_type", "hyperion.ports.keyval"),
    ("hyperion.infrastructure.keyval", "InMemoryStore", "hyperion.adapters.keyval.memory"),
    ("hyperion.infrastructure.keyval", "DynamoDBStore", "hyperion.adapters.keyval.dynamodb"),
    # message_queue: abstract -> ports, models -> domain, adapters -> adapters
    ("hyperion.infrastructure.message_queue", "Queue", "hyperion.ports.queue"),
    ("hyperion.infrastructure.message_queue", "Message", "hyperion.domain.messages"),
    ("hyperion.infrastructure.message_queue", "ArrivalEvent", "hyperion.domain.messages"),
    ("hyperion.infrastructure.message_queue", "DataLakeArrivalMessage", "hyperion.domain.messages"),
    ("hyperion.infrastructure.message_queue", "SourceBackfillMessage", "hyperion.domain.messages"),
    ("hyperion.infrastructure.message_queue", "SerializedMessage", "hyperion.domain.messages"),
    (
        "hyperion.infrastructure.message_queue",
        "iter_messages_from_sqs_event",
        "hyperion.domain.messages",
    ),
    ("hyperion.infrastructure.message_queue", "create_backfill_event", "hyperion.domain.messages"),
    ("hyperion.infrastructure.message_queue", "InMemoryQueue", "hyperion.adapters.queue.memory"),
    ("hyperion.infrastructure.message_queue", "FileQueue", "hyperion.adapters.queue.filesystem"),
    ("hyperion.infrastructure.message_queue", "SQSQueue", "hyperion.adapters.queue.sqs"),
    # secrets
    ("hyperion.infrastructure.secretsmanager", "SecretsManager", "hyperion.ports.secrets"),
    ("hyperion.infrastructure.secretsmanager", "SECRET_PATTERN", "hyperion.ports.secrets"),
    ("hyperion.infrastructure.secretsmanager", "DummySecretsManager", "hyperion.adapters.secrets.dummy"),
    ("hyperion.infrastructure.secretsmanager", "AWSSecretsManager", "hyperion.adapters.secrets.aws_sm"),
    # http
    ("hyperion.infrastructure.httputils", "redact_url", "hyperion.adapters.http.proxy"),
    ("hyperion.infrastructure.httputils", "get_proxy_mounts", "hyperion.adapters.http.proxy"),
    # schema registry
    ("hyperion.catalog.schema", "SchemaStore", "hyperion.ports.schema_registry"),
    ("hyperion.catalog.schema", "AVRO_SCHEMAS_PATH", "hyperion.adapters.schema_registry.local"),
    ("hyperion.catalog.schema", "LocalSchemaStore", "hyperion.adapters.schema_registry.local"),
    ("hyperion.catalog.schema", "S3SchemaStore", "hyperion.adapters.schema_registry.s3"),
    # S7: geocoder + PersistentCache knot
    ("hyperion.infrastructure.geo.gmaps", "GoogleMaps", "hyperion.adapters.geocoder.google"),
    ("hyperion.infrastructure.geo", "GoogleMaps", "hyperion.adapters.geocoder.google"),
    ("hyperion.infrastructure.cache", "PersistentCache", "hyperion.application.persistent_cache"),
]


@pytest.mark.parametrize(("old_module", "attr", "new_module"), RELOCATIONS)
def test_old_path_warns_and_resolves_to_new_object(old_module: str, attr: str, new_module: str) -> None:
    old_mod = importlib.import_module(old_module)
    new_obj = getattr(importlib.import_module(new_module), attr)

    with pytest.warns(DeprecationWarning, match=rf"{new_module}\b"):
        old_obj = getattr(old_mod, attr)

    assert old_obj is new_obj


def test_unknown_attribute_still_raises_attribute_error() -> None:
    from hyperion.infrastructure import keyval

    with pytest.raises(AttributeError, match="no attribute 'DoesNotExist'"):
        keyval.DoesNotExist  # noqa: B018
