"""Composition root.

This is the **only** module allowed to import from both ``hyperion.ports`` and
``hyperion.adapters`` and to construct concrete adapters from environment
configuration (DDD refactor F9 / Step 8, ``docs/ddd-refactor-plan.md`` layering
rule 6). Every other module depends on the abstract ports only; the
``*.from_config`` classmethods on the port classes delegate here so backend
selection lives in exactly one place.

Selection is config-driven and the chosen adapter is imported *inside* its
branch -- so a lite install (no ``[aws]`` extra) whose config selects a
memory/filesystem backend never imports ``boto3``. These functions are pure
constructors: they do **not** cache. Singleton / instance reuse stays on the
port wrappers (``Cache._instances``, ``Queue._cached_instance``,
``SecretsManager._instance``, ``SchemaStore._instances``) so observable
behaviour is unchanged.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from hyperion.config import geo_config, queue_config, secrets_config, storage_config
from hyperion.log import get_logger

if TYPE_CHECKING:
    from hyperion.domain.assets import AssetType
    from hyperion.ports.cache import Cache
    from hyperion.ports.geocoder import Geocoder
    from hyperion.ports.keyval import KeyValueStore
    from hyperion.ports.queue import Queue
    from hyperion.ports.schema_registry import SchemaStore
    from hyperion.ports.secrets import SecretsManager
    from hyperion.ports.storage import StoragePort

logger = get_logger("composition")

# Queue backend name resolution -- intentionally importing no adapter so the
# Queue config-consistency check (which runs on every ``Queue.from_config``)
# stays boto3-free.
QUEUE_BACKEND_CLASS_NAMES = {
    "sqs": "SQSQueue",
    "file": "FileQueue",
    "memory": "InMemoryQueue",
}


def resolve_queue_backend() -> str:
    """Resolve the queue backend key (``"sqs"`` | ``"file"`` | ``"memory"``).

    Mirrors the pre-refactor ``Queue._resolve_type_from_config`` 1:1, including
    the both-URL-and-path ambiguity warning, *without importing any adapter*.
    """
    if queue_config.url and queue_config.path:
        logger.warning(
            "Ambiguous configuration detected - both queue URL and queue path were set. "
            "Will default to SQS queue.",
            queue_url=queue_config.url,
            queue_path=queue_config.path,
        )
    if queue_config.url is not None:
        return "sqs"
    if queue_config.path is not None:
        return "file"
    return "memory"


def default_cache() -> Cache:
    """Construct the cache adapter selected by configuration."""
    if storage_config.cache_dynamodb_table:
        from hyperion.adapters.cache.dynamodb import DynamoDBCache

        logger.info("Using DynamoDB Cache.")
        return DynamoDBCache(
            prefix=storage_config.cache_key_prefix,
            default_ttl=storage_config.cache_dynamodb_default_ttl,
            table_name=storage_config.cache_dynamodb_table,
        )
    if storage_config.cache_local_path:
        from hyperion.adapters.cache.filesystem import LocalFileCache

        logger.info("Using LocalFileCache.", path=storage_config.cache_local_path)
        return LocalFileCache(
            prefix=storage_config.cache_key_prefix,
            root_path=Path(storage_config.cache_local_path),
        )
    from hyperion.adapters.cache.memory import InMemoryCache

    logger.info("Using InMemory Cache.")
    return InMemoryCache(
        prefix=storage_config.cache_key_prefix,
        default_ttl=storage_config.cache_dynamodb_default_ttl,
    )


def default_queue() -> Queue:
    """Construct the queue adapter selected by configuration."""
    backend = resolve_queue_backend()
    logger.info("Resolved queue type from configuration.", queue_backend=backend)
    if backend == "sqs" and queue_config.url is not None:
        from hyperion.adapters.queue.sqs import SQSQueue

        logger.info("Using SQS queue.")
        return SQSQueue(queue_config.url)
    if backend == "file" and queue_config.path is not None:
        from hyperion.adapters.queue.filesystem import FileQueue

        logger.info("Using FileQueue.")
        return FileQueue(Path(queue_config.path), overwrite=queue_config.path_overwrite)
    if backend == "memory":
        from hyperion.adapters.queue.memory import InMemoryQueue

        logger.info("Using in-memory queue.")
        return InMemoryQueue()
    # Unreachable: resolve_queue_backend only returns sqs/file/memory and the
    # url/path guards above hold by construction. Kept as a defensive mirror of
    # the pre-refactor Queue._create_from_config final raise.
    raise ValueError(  # pragma: no cover
        f"Unknown queue backend {backend!r} or missing configuration options."
    )


def default_secrets() -> SecretsManager:
    """Construct the secrets-manager adapter selected by configuration."""
    if secrets_config.backend is None:
        from hyperion.adapters.secrets.dummy import DummySecretsManager

        logger.warning("No secrets backend is configured. Using dummy secrets manager.")
        return DummySecretsManager()
    if secrets_config.backend == "AWSSecretsManager":
        from hyperion.adapters.secrets.aws_sm import AWSSecretsManager

        logger.info("Using AWS Secrets Manager.")
        return AWSSecretsManager()
    raise ValueError(f"Unsupported secrets backend: {secrets_config.backend!r}.")


def default_schema_registry(path: str | None = None) -> SchemaStore:
    """Construct the schema-store adapter for ``path`` (or the configured one).

    Scheme dispatch is identical to the pre-refactor
    ``SchemaStore._create_new``: ``file://`` / no scheme -> ``LocalSchemaStore``,
    ``s3://`` -> ``S3SchemaStore``.
    """
    resolved_path = path if path is not None else storage_config.schema_path
    parsed = urlparse(resolved_path)
    if parsed.scheme == "file" or not parsed.scheme:
        from hyperion.adapters.schema_registry.local import LocalSchemaStore

        resolved = (Path(parsed.netloc or "/") / parsed.path.lstrip("/")).resolve()
        logger.info("Using file schema store.", path=resolved.as_posix())
        return LocalSchemaStore(resolved)
    if parsed.scheme == "s3":
        from hyperion.adapters.schema_registry.s3 import S3SchemaStore

        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        logger.info("Using S3 schema store.", bucket=bucket, prefix=prefix)
        return S3SchemaStore(bucket, prefix)
    logger.critical("Unsupported schema store scheme.", scheme=parsed.scheme, path=resolved_path)
    raise ValueError(f"Unsupported schema store scheme {parsed.scheme!r}.")


def default_storage() -> dict[AssetType, StoragePort]:
    """Per-asset-type storage map for ``Catalog.from_config``.

    One :class:`~hyperion.adapters.storage.s3.S3Storage` per asset type, so the
    on-S3 key layout (bucket + prefix per store) is identical to the
    pre-refactor catalog. Storage is always S3 today (no memory/filesystem
    fallback in ``from_config`` -- unchanged behaviour).
    """
    from hyperion.adapters.storage.s3 import S3Storage

    def _prefix(value: str) -> str:
        value = value.strip("/")
        return f"{value}/" if value else ""

    return {
        "data_lake": S3Storage(storage_config.data_lake_bucket, _prefix(storage_config.data_lake_prefix)),
        "feature": S3Storage(storage_config.feature_store_bucket, _prefix(storage_config.feature_store_prefix)),
        "persistent_store": S3Storage(
            storage_config.persistent_store_bucket, _prefix(storage_config.persistent_store_prefix)
        ),
    }


def default_keyval() -> KeyValueStore:
    """Construct the default key-value store.

    No keyval configuration exists today and Step 8 adds none (no new config
    keys, no behaviour change). This mirrors ``GoogleMaps.__init__``'s current
    implicit default. Future config-driven backend selection slots in here
    without touching call sites.
    """
    from hyperion.adapters.keyval.memory import InMemoryStore

    return InMemoryStore()


def default_geocoder() -> Geocoder:
    """Construct the geocoder adapter selected by configuration.

    The composition-root entry point for the geocoder. Behaviourally identical
    to ``GoogleMaps.from_config`` (fresh instance, no singleton, same missing-key
    ``ValueError``); the geocode cache uses :func:`default_keyval`, functionally
    identical to the adapter's implicit ``InMemoryStore`` default. The adapter's
    own ``GoogleMaps.from_config`` is intentionally left self-contained -- per
    layering rule 6 an adapter must not import this composition module -- so it
    does not route through here.
    """
    if geo_config.gmaps_api_key is None:
        raise ValueError("Google Maps API key is not set.")
    from hyperion.adapters.geocoder.google import GoogleMaps

    return GoogleMaps(api_key=geo_config.gmaps_api_key, keyval=default_keyval())
