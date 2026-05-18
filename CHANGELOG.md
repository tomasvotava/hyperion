# Changelog

## Upgrade guide — DDD ports/adapters refactor

The next major release finishes the ports/adapters (DDD) layout. It is a
**structural** refactor — no features added, no behaviour changed (one bug
fixed, see below). Every relocated symbol stays importable from its old path
for the whole 1.x line behind a `DeprecationWarning`; the old paths are removed
in 2.0. The single non-additive break is the `Catalog` constructor, mitigated
by `Catalog.from_config()`.

This guide is the migration reference for downstream consumers. It is
pinned above the generated release history on purpose and applies as of the
first 1.x release.

### Packaging: lite core + opt-in extras

`pip install hyperion-sdk` becomes a slim lite core. Heavy backends are opt-in
extras. Install the extras for the capabilities you use:

Lite core (always installed): `loguru`, `pydantic`, `httpx`,
`python-dateutil`, `python-dotenv`, `env-proxy`, `cachetools`, `click`,
`haversine`, `aws-lambda-typing`.

| Extra | Pulls | Enables |
|---|---|---|
| `[aws]` | `boto3`, `aioboto3` | every `adapters/*/dynamodb`, `s3`, `sqs`, `aws_sm` (DynamoDB cache/keyval, S3 storage/schema, SQS queue, AWS Secrets Manager) |
| `[data]` | `polars`, `pandera`, `numpy` | `hyperion.data.*` (pandera↔polars typing, asset schemas, `SpatialKMeans`) |
| `[catalog]` | `fastavro` | `hyperion.adapters.serialization.avro` (avro-backed `Catalog`) |
| `[geo]` | `googlemaps` | `hyperion.adapters.geocoder.google` (`GoogleMaps`); Haversine math stays lite |
| `[snappy]` | `python-snappy` | compressed filesystem cache / DynamoDB keyval (graceful no-compression fallback when absent) |
| `[all]` | union of the above | parity with the pre-refactor full install |

`[catalog]` no longer transitively requires `[aws]`: a `Catalog` backed by
local filesystem storage works with `[catalog]` alone.

Downstream `sed`/pin guidance: a consumer that used `Catalog`/avro now needs
`hyperion-sdk[catalog]`; one that used DynamoDB/S3/SQS needs `[aws]`; one that
used the pandera/polars helpers needs `[data]`; `[all]` reproduces today's
install exactly.

> The packaging change ships with the next major release. Until that release
> is published, the default install still pulls every dependency — pin the
> extras now so the upgrade is a no-op.

### Import path changes (deprecated; removed in the next major)

Every symbol below keeps working from its old path through all of 1.x (with a
`DeprecationWarning` naming the new path) and resolves to the *same* object as
the new path. 58 symbols across 12 modules moved. Migrate by rewriting the
import module — the symbol names are unchanged.

| Old import path | Symbols (names unchanged) | New import path |
|---|---|---|
| `hyperion.infrastructure.cache` | `Cache`, `CacheStats`, `CachingError` | `hyperion.ports.cache` |
| `hyperion.infrastructure.cache` | `InMemoryCache` | `hyperion.adapters.cache.memory` |
| `hyperion.infrastructure.cache` | `LocalFileCache`, `DEFAULT_LOCAL_FILE_CACHE_MAX_SIZE` | `hyperion.adapters.cache.filesystem` |
| `hyperion.infrastructure.cache` | `DynamoDBCache`, `DYNAMODB_MAX_LENGTH` | `hyperion.adapters.cache.dynamodb` |
| `hyperion.infrastructure.cache` | `PersistentCache` | `hyperion.application.persistent_cache` |
| `hyperion.infrastructure.keyval` | `KeyValueStore`, `CompressionType`, `is_valid_compression_type` | `hyperion.ports.keyval` |
| `hyperion.infrastructure.keyval` | `InMemoryStore` | `hyperion.adapters.keyval.memory` |
| `hyperion.infrastructure.keyval` | `DynamoDBStore` | `hyperion.adapters.keyval.dynamodb` |
| `hyperion.infrastructure.message_queue` | `Queue` | `hyperion.ports.queue` |
| `hyperion.infrastructure.message_queue` | `Message`, `ArrivalEvent`, `DataLakeArrivalMessage`, `SourceBackfillMessage`, `SerializedMessage`, `iter_messages_from_sqs_event`, `create_backfill_event` | `hyperion.domain.messages` |
| `hyperion.infrastructure.message_queue` | `InMemoryQueue` | `hyperion.adapters.queue.memory` |
| `hyperion.infrastructure.message_queue` | `FileQueue` | `hyperion.adapters.queue.filesystem` |
| `hyperion.infrastructure.message_queue` | `SQSQueue` | `hyperion.adapters.queue.sqs` |
| `hyperion.infrastructure.secretsmanager` | `SecretsManager`, `SECRET_PATTERN` | `hyperion.ports.secrets` |
| `hyperion.infrastructure.secretsmanager` | `DummySecretsManager` | `hyperion.adapters.secrets.dummy` |
| `hyperion.infrastructure.secretsmanager` | `AWSSecretsManager` | `hyperion.adapters.secrets.aws_sm` |
| `hyperion.infrastructure.httputils` | `redact_url`, `get_proxy_mounts` | `hyperion.adapters.http.proxy` |
| `hyperion.infrastructure.geo.gmaps` | `GoogleMaps` | `hyperion.adapters.geocoder.google` |
| `hyperion.infrastructure.geo` | `GoogleMaps` | `hyperion.adapters.geocoder.google` |
| `hyperion.infrastructure.geo.location` | `Location`, `NamedLocation`, `SpatialKMeans`, `LATITUDE_DEGREE_TO_METERS`, `EARTH_RADIUS_METERS`, `meters_to_degrees` | `hyperion.domain.geo` |
| `hyperion.catalog.schema` | `SchemaStore` | `hyperion.ports.schema_registry` |
| `hyperion.catalog.schema` | `AVRO_SCHEMAS_PATH`, `LocalSchemaStore` | `hyperion.adapters.schema_registry.local` |
| `hyperion.catalog.schema` | `S3SchemaStore` | `hyperion.adapters.schema_registry.s3` |
| `hyperion.catalog` | `SchemaStore` | `hyperion.ports.schema_registry` |
| `hyperion.catalog` | `Catalog`, `AssetNotFoundError` | `hyperion.catalog.catalog` |
| `hyperion.typeutils` | `PANDERA_TO_POLARS_MAPPING`, `POLARS_SCHEMA_COPY_ATTRIBUTES`, `PolarsUTCDateTime`, `map_pandera_dtype_to_polars` | `hyperion.data.typeutils` |
| `hyperion.entities.catalog` | `AssetType`, `AssetProtocol`, `get_prefixed_path`, `DataLakeAsset`, `PersistentStoreAsset`, `FeatureAsset` | `hyperion.domain.assets` |
| `hyperion.entities.catalog` | `FeatureModel`, `PolarsFeatureModel` | `hyperion.data.asset_schemas` |

Stayed put — do not migrate these: the lite core (`hyperion.log`,
`hyperion.config`, `hyperion.dateutils`, `hyperion.asyncutils`) is unchanged,
and `hyperion.typeutils` keeps its stdlib helpers (e.g. `DateOrDelta`,
`assert_type`, `dataclass_asdict`, `is_typed_dict_instance`). Only the
pandera/polars names listed above left `hyperion.typeutils`.

### Catalog constructor change (the one hard break)

`Catalog.__init__` is the single non-additive break — there is no compat shim
for it. The old bucket/prefix keyword arguments, the `StoreBucketConfig`
TypedDict, the `Catalog.s3_client` property and `get_store_config()` are
removed.

Old (removed):

```python
Catalog(
    data_lake_bucket=...,
    feature_store_bucket=...,
    persistent_store_bucket=...,
    data_lake_prefix="",
    feature_store_prefix="",
    persistent_store_prefix="",
    queue=None,
    cache=None,
    schema_store=None,
)
```

New:

```python
from hyperion.ports.storage import StoragePort

Catalog(
    storage=...,            # StoragePort, or Mapping[AssetType, StoragePort]
    queue=None,
    cache=None,
    schema_store=None,
    serializer=None,        # AvroSerializer; defaults to a fresh one
)
```

A single `StoragePort` serves every asset type; pass a mapping keyed by
`"data_lake"` / `"feature"` / `"persistent_store"` to route each type to a
different backend. Bucket and prefix configuration now lives inside the
`S3Storage` adapter, not on `Catalog`.

Escape hatch: `Catalog.from_config()` is unchanged in name and behaviour
intent — it reads the same environment configuration and builds one
`S3Storage` per asset type, reproducing the pre-refactor on-S3 key layout.
Callers that used the old keyword arguments only to mirror the environment
config should switch to `Catalog.from_config()` and need no further changes.

New capability unlocked by the inversion (not a new API): a `Catalog` can now
run against local disk without AWS —
`Catalog(storage=FilesystemStorage("/data"), schema_store=LocalSchemaStore("/schemas"))`
with only `hyperion-sdk[catalog]`.

Bug fixed by this change (behaviour change, called out deliberately):
pre-refactor `Catalog.from_config()` wired the **feature store** with the
**data-lake** prefix (`feature_store_prefix=storage_config.data_lake_prefix`).
It now correctly uses `feature_store_prefix`. In environments where the data
lake and feature store prefixes differ, feature-store asset keys resolve to a
different (correct) S3 path after upgrading. This is a fix, not a regression —
review any tooling that depended on the old, wrong path.

### Deprecation & removal policy

- Every old import path above is deprecated as of the first 1.x release and
  remains functional for the entire 1.x line.
- Accessing a relocated symbol from its old path emits a
  `DeprecationWarning` that names the new path and states it will be removed
  in `hyperion-sdk` 2.0.
- The old paths, the `Catalog.from_config()` compatibility default, and the
  removed `Catalog` constructor surface are deleted in 2.0. There will be at
  least one minor release of warnings before any removal.
- `PersistentCache` is relocated to `hyperion.application.persistent_cache`
  and remains functional through 1.x; it is superseded by injecting a
  `KeyValueStore` and is removed in 2.0.

### Downstream consumers

These steps apply to every consumer of `hyperion-sdk`; none is specific to any
one project. Work through them in order:

- **Relax the version pin.** Any pin that excludes the new major (for example
  `hyperion-sdk>=0.15.0,<1`) must be widened to admit it.
- **Request the extras you use.** The default install is now a lite core (see
  *Packaging* above). A consumer that uses `Catalog`/avro needs
  `hyperion-sdk[catalog]`; DynamoDB/S3/SQS needs `[aws]`; the pandera/polars
  helpers need `[data]`; Google geocoding needs `[geo]`. `[all]` reproduces the
  pre-refactor install exactly.
- **Rewrite moved imports.** Apply the *Import path changes* table above; only
  the import module changes, symbol names are unchanged. Old paths keep working
  through the whole 1.x line but emit a `DeprecationWarning` naming the new
  path — run your test suite with deprecation warnings visible to surface every
  call site mechanically.
- **Check direct `Catalog(...)` construction.** This is the only hard break.
  Code that instantiates `Catalog` with the old `*_bucket`/`*_prefix` keyword
  arguments must switch to `Catalog.from_config()` (unchanged in name and
  intent — it rebuilds the per-bucket S3 layout from the environment) or to the
  new `storage=` constructor. Consumers that only ever used
  `Catalog.from_config()` need no constructor change.
- **Account for the `from_config()` prefix fix.** Consumers that rely on
  `Catalog.from_config()` and whose data-lake and feature-store prefixes differ
  will see feature-store asset keys resolve to the corrected path (see *Catalog
  constructor change* above). Review any tooling that depended on the old path.

## 1.0.0 (2026-05-18)

### BREAKING CHANGE

- default install is now lite (opt into [aws]/[data]/[catalog]/[geo]/[snappy]); minimum Python is now 3.11 (3.10 dropped); some import paths moved (see CHANGELOG migration table).

### Refactor

- **catalog**: make hyperion.catalog import lazy
- **composition**: centralize from_config wiring
- **geocoder**: remove PersistentCache knot, inject KeyValueStore
- **adapters**: relocate cache/keyval/queue/secrets/schema impls
- **catalog**: depend on StoragePort, extract avro serializer
- **storage**: add StoragePort + memory/filesystem/s3 adapters
- **geo**: extract pure-domain Location, drop cache singleton
- split typeutils and asset identities from the data stack
- **ports**: extract abstract bases into hyperion/ports


- extras model + import-linter + slim default install

## 0.15.1 (2026-05-12)

### Fix

- add type annotation for cache attribute

## 0.15.0 (2026-03-11)

### Feat

- remove AsyncHTTPClientWrapper, promote `get_proxy_mounts` to public method

## 0.14.1 (2025-11-28)

### Fix

- FileQueue calls __repr__ before assigning attributes

## 0.14.0 (2025-11-26)

### Feat

- add support for python 3.14

## 0.13.0 (2025-11-05)

### Feat

- allow setting http proxy for https and vice-versa

## 0.12.0 (2025-10-02)

### Feat

- add schema_path to DataLakeArrivalMessage and update notification method

### Fix

- make sure attributes exist when logging self

## 0.11.0 (2025-09-29)

### Feat

- add iter_intervals to dateutils

## 0.10.0 (2025-08-27)

### Feat

- allow testing Sources with lambda events locally, add support for source backfill
- add CLI for running sources with configurable parameters and file queue
- make sources able to run independently from lambda (e.g. in Argo Workflows), add optional `params` to Source

## 0.9.1 (2025-07-04)

### Fix

- source asset should pass schema_path to catalog

## 0.9.0 (2025-07-04)

### Feat

- add support for custom schemas in asset storage

## 0.8.0 (2025-06-04)

### Feat

- add stats for cache implementations

## 0.7.1 (2025-06-02)

### Fix

- ensure cached asset is not closed before read

## 0.7.0 (2025-05-21)

### Feat

- add asset caching to Catalog
- add __repr__ method to SchemaStore for better debugging
- add open method to Cache for file-like access with string and bytes modes
- extend Cache interface with get_bytes and set_bytes

## 0.6.1 (2025-03-28)

### Fix

- map pandera to polars dtype manually to create empty DataFrame/LazyFrame with the correct schema

## 0.6.0 (2025-03-27)

### Feat

- add Polars support for feature models and asset collections

## 0.5.1 (2025-03-18)

### Fix

- ensure start date is timezone-aware in asset collection methods

## 0.5.0 (2025-03-13)

### Feat

- add method `get_altitude` to retrieve altitude using Google Maps Elevation API

## 0.4.0 (2025-03-13)

### Feat

- add support for python 3.13

### Fix

- remove unnecessary 'v' prefix from version tags in release workflows

## 0.3.0 (2025-03-12)

### BREAKING CHANGE

- `hyperion.logging` renamed to `hyperion.log`
- `hyperion.infrastructure.queue` renamed to `hyperion.infrastructure.message_queue`
- `hyperion.infrastructure.secrets` renamed to `hyperion.infrastructure.secretsmanager`
- `hyperion.infrastructure.http` renamed to `hyperion.infrastructure.httputils`
- `hyperion.collections` moved to `hyperion.repository`

### Fix

- **ci**: remove check for changed files after cz bump that in fact blocks release

### Refactor

- update import paths for logging and asset collection modules

## 0.2.0 (2025-03-12)

### Feat

- Add `AssetCollection` and `FeatureFetchSpecifier` functionality
- Add `FeatureModel` base class for type-safe feature models

## 0.1.0 (2025-03-05)

### Feat

- catalog, schema store, caching and infrastructure helpers
- add README file with project description
- integrate SchemaStore for asset schema retrieval in catalog components
- add delete method to SQSQueue for message removal
- implement delete method in Queue and InMemoryQueue for message removal
- add SecretsConfig and implement SecretsManager for environment variable secret management
- implement base source class and SQS event handling for message processing
- enhance run method to support both Awaitable and AsyncIterator for improved asset processing
- add meters to degrees conversion function for geographical calculations
- add LocalFileCache implementation for file-based caching and update StorageConfig for local cache path
- update SourceBackfillMessage to use start_date and end_date
- add iter_dates_between function to generate dates between two datetimes with specified granularity
- add iter_async function to create an asynchronous iterator from a given iterable
- implement asset repartitioning functionality in Catalog class
- add async iterator for handling dates in SourceAsset class
- update iter_dates_between to accept datetime.date in addition to datetime.datetime
- add asynchronous upload method to S3Client using aioboto3
- add async utilities for asynchronous task handling
- add GitHub Actions workflow for running pytest on multiple Python versions
- add Codecov action to upload coverage reports in GitHub Actions workflow
- enhance AsyncTaskQueue with maxsize handling and logging
- implement SpatialKMeans for geographical clustering and enhance Location class with distance methods
- add create_backfill_event function to generate SQSEvent for SourceBackfillMessage
- update repartition method to accept optional data parameter for asset processing
- add HttpConfig class and AsyncHTTPClientWrapper for improved HTTP handling and proxy support
- enhance truncate_datetime function to accept datetime.date and convert to datetime
- make SpatialKMeans and Location classes generic to support various location types
- add get_feature_data method to Catalog class for retrieving feature data with time resolution
- add get_loop function to manage asyncio event loops
- add dataclass_asdict function for converting dataclass instances to dictionaries
- implement key-value store with in-memory and DynamoDB backends
- add type guard for valid compression types in key-value store
- add methods to iterate and find latest data lake partitions
- enhance LocalFileCache with max size limit and cleanup functionality
- add asyncio lock to AsyncHTTPClientWrapper for thread safety
- add get_date_pattern function for formatted date strings based on time resolution
- add iter_feature_store_partitions method to iterate over relevant feature store partitions
- add DateOrDelta type alias for datetime and timedelta
- add utcnow dateutil function
- replace datetime.now with utcnow in various classes
- add reverse geocoding functionality and NamedLocation data class

### Fix

- move install-all action and pre-commit workflow
- correct type hint syntax for _instance in SecretsManager class
- update logging to reference asset property in Source class
- update Source class to enforce source name implementation during instantiation
- pass dynamodb table name to DynamoDBCache from env var correctly
- implement singleton pattern for GoogleMaps class instantiation
- simplify expiration check in DynamoDBCache get_item method
- enhance assure_timezone function to handle existing timezone awareness
- update assure_timezone function to handle datetime.date input
- update secret handling in moto server configuration for test environment
- make private attributes accessible
- change logging level from info to debug for nearest location found
- use get_loop function for event loop management in Source class
- update asset type in AssetRepartitioner to use AssetProtocol
- rename timestamp attribute to partition_date in FeatureAsset class
- remove unnecessary threshold check in nearest location calculation
- make include argument in dataclass_asdict function optional
- prefix cache key even if hashing is off
