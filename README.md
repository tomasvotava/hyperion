# Hyperion

A headless ETL / ELT / data pipeline and integration SDK for Python.

[![pre-commit](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml)
[![pytest](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/tomasvotava/hyperion/branch/master/graph/badge.svg?token=your-token)](https://codecov.io/gh/tomasvotava/hyperion)

## Features

- **Data Catalog System**: Manage and organize data assets across S3 buckets
- **Schema Management**: Validate and store schema definitions for data assets
- **AWS Infrastructure Abstractions**: Simplified interfaces for S3, DynamoDB, SQS, and Secrets Manager
- **Source Framework**: Define data sources that extract data and store in the catalog
- **Caching**: In-memory, local file, and DynamoDB caching options
- **Asynchronous Processing**: Utilities for async operations and task queues
- **Geo Utilities**: Location-based services with Google Maps integration

## Installation

Hyperion uses [Poetry](https://python-poetry.org/) for dependency management:

```bash
# Install poetry if you don't have it
curl -sSL https://install.python-poetry.org | python3 -

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
HYPERION_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue

# Secrets settings
HYPERION_SECRETS_BACKEND=AWSSecretsManager

# HTTP settings (optional)
HYPERION_HTTP_PROXY_HTTP=http://proxy:8080
HYPERION_HTTP_PROXY_HTTPS=http://proxy:8080

# Geo settings (optional)
HYPERION_GEO_GMAPS_API_KEY=your-google-maps-api-key
```

## Basic Usage

### Creating and Storing Assets

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import DataLakeAsset
from datetime import datetime, timezone

# Initialize the catalog
catalog = Catalog.from_config()

# Create a data lake asset
asset = DataLakeAsset(
    name="my-data",
    date=datetime.now(timezone.utc),
    schema_version=1
)

# Store data in the asset
data = [
    {"id": 1, "name": "Item 1", "timestamp": datetime.now(timezone.utc)},
    {"id": 2, "name": "Item 2", "timestamp": datetime.now(timezone.utc)},
]

catalog.store_asset(asset, data)
```

### Retrieving Assets

```python
from hyperion.catalog import Catalog
from hyperion.entities.catalog import DataLakeAsset
from datetime import datetime, timezone

# Initialize the catalog
catalog = Catalog.from_config()

# Create asset reference
asset = DataLakeAsset(
    name="my-data",
    date=datetime.now(timezone.utc),
    schema_version=1
)

# Retrieve data
for item in catalog.retrieve_asset(asset):
    print(item)
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

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `poetry run pytest`
5. Run pre-commit hooks: `poetry run pre-commit run -a`
6. Commit your changes using [Conventional Commits](https://www.conventionalcommits.org/)
7. Push to your branch: `git push origin feature/my-feature`
8. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
