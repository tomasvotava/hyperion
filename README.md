# Hyperion

A headless ETL / ELT / data pipeline and integration SDK for Python.

[![pre-commit](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml)
[![pytest](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/tomasvotava/hyperion/branch/master/graph/badge.svg?token=your-token)](https://codecov.io/gh/tomasvotava/hyperion)
![PyPI - License](https://img.shields.io/pypi/l/hyperion-sdk)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hyperion-sdk)
![GitHub Release](https://img.shields.io/github/v/release/tomasvotava/hyperion)

## Features

- **Data Catalog System**: Manage and organize data assets across S3 buckets
- **Schema Management**: Validate and store schema definitions for data assets
- **AWS Infrastructure Abstractions**: Simplified interfaces for S3, DynamoDB, SQS, and Secrets Manager
- **Source Framework**: Define data sources that extract data and store in the catalog
- **Caching**: In-memory, local file, and DynamoDB caching options
- **Asynchronous Processing**: Utilities for async operations and task queues
- **CLI Runner**: Run sources in standalone or Argo Workflow mode with click-based commands
- **Geo Utilities**: Location-based services with Google Maps integration
- **Catalog Caching**: Cache downloaded assets for faster repeated access
- **Asset Collections**: High-level interface for working with groups of assets

## Core Concepts

### Assets

Assets are the fundamental units of data in Hyperion. Each asset represents a dataset stored in a specific location with a defined schema. Hyperion supports three types of assets:

#### DataLakeAsset

- Represents raw, immutable data stored in a data lake
- Time-partitioned by date
- Each partition has a schema version
- Example use cases: raw API responses, event logs, or any source data that needs to be preserved in its original form

#### FeatureAsset

- Represents processed feature data with time resolution
- Used for analytics, machine learning features, and derived datasets
- Supports different time resolutions (seconds, minutes, hours, days, weeks, months, years)
- Can include additional partition keys for finer-grained organization
- Example use cases: aggregated metrics, processed signals, ML features

#### PersistentStoreAsset

- Represents persistent data storage without time partitioning
- Used for reference data, lookup tables, or any data that doesn't change frequently
- Example use cases: reference data, configuration settings, master data

### Schema Management

All assets in Hyperion have associated schemas that define their structure:

- **Schema Store**: The SchemaStore manages asset schemas in Avro format
- **Schema Validation**: All data is validated against its schema during storage
- **Schema Versioning**: Assets include a schema version to support evolution over time
- **Schema Storage**: Schemas can be stored in the local filesystem or S3

If a schema is missing for an asset:

1. An error will be raised when attempting to store or retrieve the asset
2. You need to define the schema in Avro format and store it in the schema store
3. The schema should be named according to the pattern: `{asset_type}/{asset_name}.v{version}.avro.json`

### Catalog

The Catalog is the central component that manages asset storage and retrieval:

- **Storage Location**: Maps asset types to their appropriate storage buckets
- **Asset Retrieval**: Provides methods to retrieve assets by name, date, and schema version
- **Partitioning**: Handles partitioning logic for different asset types
- **Notifications**: Can send notifications when new assets arrive
- **Caching**: Can use a cache for faster repeated retrieval of assets

### Source Framework

Sources are responsible for extracting data from external systems and storing it in the catalog:

- **Standardized Interface**: All sources implement a common interface
- **AWS Lambda Support**: Easy integration with AWS Lambda for scheduled extraction
- **Backfill Capability**: Support for historical data backfill
- **Incremental Processing**: Extract data with date-based filtering

## Installation

### From PyPI

If you only want to use `hyperion`, you can install it from PyPI:

```bash
pip install hyperion-etl
# or
poetry add hyperion-etl
```

We'll try to respect [Semantic Versioning](https://semver.org/), so you can pin your dependencies to a specific version,
or use a version selector to make sure no breaking changes are introduced (e.g., `^0.4.0`).

### From Source

Hyperion uses [Poetry](https://python-poetry.org/) for dependency management:

```bash
# Clone the repository
git clone https://github.com/tomasvotava/hyperion.git
cd hyperion

# Install dependencies
poetry install
```

## Configuration

Hyperion is configured through environment variables. You can use a `.env` file for local development:

```bash
# Common settings
HYPERION_COMMON_LOG_PRETTY=True
HYPERION_COMMON_LOG_LEVEL=INFO
HYPERION_COMMON_SERVICE_NAME=my-service

# Storage settings
HYPERION_STORAGE_DATA_LAKE_BUCKET=my-data-lake-bucket
HYPERION_STORAGE_FEATURE_STORE_BUCKET=my-feature-store-bucket
HYPERION_STORAGE_PERSISTENT_STORE_BUCKET=my-persistent-store-bucket
HYPERION_STORAGE_SCHEMA_PATH=s3://my-schema-bucket/schemas
HYPERION_STORAGE_MAX_CONCURRENCY=5

# Queue settings
## SQS Queue
HYPERION_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
## OR file queue (local)
HYPERION_QUEUE_PATH=/tmp/hyperion-queue.json
HYPERION_QUEUE_PATH_OVERWRITE=True                # Overwrite queue file if exists (default: False)

# Secrets settings
HYPERION_SECRETS_BACKEND=AWSSecretsManager

# HTTP settings (optional)
HYPERION_HTTP_PROXY_HTTP=http://proxy:8080
HYPERION_HTTP_PROXY_HTTPS=http://proxy:8080

# Geo settings (optional)
HYPERION_GEO_GMAPS_API_KEY=your-google-maps-api-key

# Cache settings
HYPERION_STORAGE_CACHE_DYNAMODB_TABLE=my-cache-table  # Optional DynamoDB table for caching
HYPERION_STORAGE_CACHE_DYNAMODB_DEFAULT_TTL=3600      # Default TTL for cache items (seconds)
HYPERION_STORAGE_CACHE_LOCAL_PATH=/tmp/hyperion-cache # Path for local file cache
HYPERION_STORAGE_CACHE_KEY_PREFIX=my-prefix           # Optional prefix for cache keys

# Source parameters (optional)
HYPERION_SOURCE_PARAMS={"key": "value"}           # Pass JSON as environment variable to the source
```

Before any real documentation is written, you can check the
[`hyperion/config.py`](hyperion/config.py) file for all available configuration options. Hyperion is using [`EnvProxy`](https://github.com/tomasvotava/env-proxy) for configuration.

## Usage Examples

### Working with Assets

#### Creating and Storing a DataLakeAsset

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import DataLakeAsset
from datetime import datetime, timezone

# Initialize the catalog
catalog = Catalog.from_config()

# Create a data lake asset
asset = DataLakeAsset(
    name="customer_data",
    date=datetime.now(timezone.utc),
    schema_version=1
)

# Store data in the asset
data = [
    {"id": 1, "name": "Customer 1", "timestamp": datetime.now(timezone.utc)},
    {"id": 2, "name": "Customer 2", "timestamp": datetime.now(timezone.utc)},
]

catalog.store_asset(asset, data)
```

#### Working with FeatureAssets

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import FeatureAsset
from hyperion.dateutils import TimeResolution
from datetime import datetime, timezone

# Initialize the catalog
catalog = Catalog.from_config()

# Create a feature asset with daily resolution
resolution = TimeResolution(1, "d")  # 1 day resolution
asset = FeatureAsset(
    name="customer_activity",
    partition_date=datetime.now(timezone.utc),
    resolution=resolution,
    schema_version=1
)

# Store aggregated feature data
feature_data = [
    {"customer_id": 1, "activity_score": 87.5, "date": datetime.now(timezone.utc)},
    {"customer_id": 2, "activity_score": 92.1, "date": datetime.now(timezone.utc)},
]

catalog.store_asset(asset, feature_data)

# Retrieve feature data for a specific time period
from_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
to_date = datetime(2023, 1, 31, tzinfo=timezone.utc)

for feature_asset in catalog.iter_feature_store_partitions(
    feature_name="customer_activity",
    resolution="1d",  # Can use string format too
    date_from=from_date,
    date_to=to_date
):
    data = catalog.retrieve_asset(feature_asset)
    for record in data:
        print(record)
```

#### Working with PersistentStoreAssets

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import PersistentStoreAsset

# Initialize the catalog
catalog = Catalog.from_config()

# Create a persistent store asset
asset = PersistentStoreAsset(
    name="product_catalog",
    schema_version=1
)

# Store reference data
products = [
    {"id": "P001", "name": "Product 1", "category": "Electronics"},
    {"id": "P002", "name": "Product 2", "category": "Clothing"},
]

catalog.store_asset(asset, products)

# Retrieve reference data
for product in catalog.retrieve_asset(asset):
    print(product)
```

#### Using a Cached Catalog

```python
from hyperion.catalog import Catalog
from hyperion.infrastructure.cache import LocalFileCache
from pathlib import Path

# Create a catalog with a local file cache
cached_catalog = Catalog(
    data_lake_bucket="my-data-lake-bucket",
    feature_store_bucket="my-feature-store-bucket",
    persistent_store_bucket="my-persistent-store-bucket",
    cache=LocalFileCache("catalog", default_ttl=3600, root_path=Path("/tmp/hyperion-cache"))
)

# First retrieval will download from S3
data = list(cached_catalog.retrieve_asset(asset))

# Subsequent retrievals will use the cache
data_again = list(cached_catalog.retrieve_asset(asset))  # Much faster!
```

### Creating a Custom Source

```python
import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

from hyperion.catalog import Catalog
from hyperion.entities.catalog import DataLakeAsset
from hyperion.sources.base import Source, SourceAsset


class MyCustomSource(Source):
    source = "my-custom-source"

    async def run(self, start_date=None, end_date=None) -> AsyncIterator[SourceAsset]:
        # Fetch your data (this is where you'd implement your data extraction logic)
        data = [
            {"id": 1, "name": "Item 1", "timestamp": datetime.now(timezone.utc)},
            {"id": 2, "name": "Item 2", "timestamp": datetime.now(timezone.utc)},
        ]

        # Create asset
        asset = DataLakeAsset(
            name="my-custom-data",
            date=datetime.now(timezone.utc)
        )

        # Yield source asset
        yield SourceAsset(asset=asset, data=data)


# Use with AWS Lambda
def lambda_handler(event, context):
    MyCustomSource.handle_aws_lambda_event(event, context)


# Use standalone
if __name__ == "__main__":
    asyncio.run(MyCustomSource._run(Catalog.from_config()))
```

#### Running source directly ('Argo Workflow' mode)

Hyperion provides a `SourceRunner` that generates CLI for your sources.

`running_sources.py`

```python
from hyperion.sources.cli import SourceRunner
from my_sources import FirstSource, SecondSource

if __name__ == "__main__":
    SourceRunner(FirstSource, SecondSource).cli()
```

You can then invoke your source from the command line:

```bash
python running_sources.py <first-source | second-source> run \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --params '{"foo": "bar"}' \
    --queue-file /argo/output/messages.json \
    --queue-overwrite
```

Generated options (all options are optional):

- `--start-date` start date passed on to the source
- `--end-date` end date passed on to the source
- `--params` JSON string containing source-specific params
- `--params-from` a path to JSON file containing source-specific params
- `--queue-file` output file for queued messages
- `--queue-overwrite` overwrite queue file if it already exists

The name of the command (`first-source`, `second-source`, etc.) comes from the `source` attribute of your
`Source` subclass.

### Working with Schemas

To create and register a schema for an asset:

```python
import json
from pathlib import Path

# Define schema in Avro format
schema = {
    "type": "record",
    "name": "CustomerData",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    ]
}

# Save schema to local file
schema_path = Path("schemas/data_lake/customer_data.v1.avro.json")
schema_path.parent.mkdir(parents=True, exist_ok=True)
with open(schema_path, "w") as f:
    json.dump(schema, f)

# Or upload to S3 if using S3SchemaStore
import boto3
s3_client = boto3.client('s3')
s3_client.put_object(
    Bucket="my-schema-bucket",
    Key="data_lake/customer_data.v1.avro.json",
    Body=json.dumps(schema)
)
```

## Advanced Features

### Asset Collections

Asset collections provide a high-level interface for fetching and working with groups of assets. You can define a collection class that specifies the assets you need and fetch them all at once.

See [`docs/asset_collections.md`](docs/asset_collections.md) for more information.

### Repartitioning Data

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import DataLakeAsset
from hyperion.dateutils import TimeResolutionUnit
from datetime import datetime, timezone
import asyncio

async def repartition_data():
    catalog = Catalog.from_config()

    # Original asset with day-level partitioning
    asset = DataLakeAsset(
        name="web_logs",
        date=datetime.now(timezone.utc),
        schema_version=1
    )

    # Repartition by hour
    await catalog.repartition(
        asset,
        granularity=TimeResolutionUnit("h"),
        date_attribute="timestamp"
    )

asyncio.run(repartition_data())
```

### Caching

Hyperion provides two types of caching:

1. **Key-Value Cache**: For general-purpose caching of any data
2. **Catalog Asset Cache**: For efficient retrieval of assets from storage

#### Key-Value Cache

```python
from hyperion.infrastructure.cache import Cache

# Get cache from configuration
cache = Cache.from_config()

# Store data in cache
cache.set("my-key", "my-value")

# Retrieve data from cache
value = cache.get("my-key")
print(value)  # "my-value"

# Check if key exists
if cache.hit("my-key"):
    print("Cache hit!")

# Delete key
cache.delete("my-key")
```

#### Catalog Asset Cache

```python
from hyperion.catalog import Catalog
from hyperion.infrastructure.cache import LocalFileCache

# Create a catalog with a local file cache
catalog = Catalog(
    data_lake_bucket="my-data-lake-bucket",
    feature_store_bucket="my-feature-store-bucket",
    persistent_store_bucket="my-persistent-store-bucket",
    cache=LocalFileCache("catalog", root_path="/tmp/hyperion-cache")
)

# Now asset retrievals will be cached
# First time: downloads from S3
data1 = list(catalog.retrieve_asset(asset1))

# Second time: uses cache
data1_again = list(catalog.retrieve_asset(asset1))  # Much faster!

# Different asset: downloads from S3
data2 = list(catalog.retrieve_asset(asset2))

# Force bypass cache for a specific retrieval
with catalog._get_asset_file_handle(asset, no_cache=True) as file:
    # Process file directly without using cache
    pass
```

### Geo Utilities

```python
from hyperion.infrastructure.geo import GoogleMaps, Location

# Initialize Google Maps client
gmaps = GoogleMaps.from_config()

# Geocode an address
with gmaps:
    location = gmaps.geocode("1600 Amphitheatre Parkway, Mountain View, CA")
    print(f"Latitude: {location.latitude}, Longitude: {location.longitude}")

    # Reverse geocode a location
    named_location = gmaps.reverse_geocode(location)
    print(f"Address: {named_location.address}")
    print(f"Country: {named_location.country}")
```

## Development

### Setup Development Environment

```bash
# Install development dependencies
poetry install

# Install pre-commit hooks
poetry run pre-commit install
```

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=hyperion

# Run specific test files
poetry run pytest tests/test_asyncutils.py
```

### Code Style

This project uses [pre-commit](https://pre-commit.com/) hooks to enforce code style:

```bash
# Run pre-commit on all files
poetry run pre-commit run -a
```

The project uses:

- [ruff](https://github.com/charliermarsh/ruff) for linting
- [mypy](https://mypy.readthedocs.io/) for type checking
- [commitizen](https://github.com/commitizen-tools/commitizen) for standardized commits

## Architecture

### Core Components

- **Catalog**: Manages data assets and their storage in S3
- **SchemaStore**: Handles schema validation and storage
- **Source**: Base class for implementing data sources
- **Infrastructure**: Abstractions for AWS services (S3, DynamoDB, SQS, etc.)
- **Utils**: Helper functions for dates, async operations, etc.

### Asset Types

- **DataLakeAsset**: Raw data stored in a data lake
- **FeatureAsset**: Processed features with time resolution
- **PersistentStoreAsset**: Persistent data storage

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on contributing to this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
