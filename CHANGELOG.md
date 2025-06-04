# Changelog

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
