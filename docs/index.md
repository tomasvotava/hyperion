# Hyperion

A headless ETL / ELT / data pipeline and integration SDK for Python.

[![pre-commit](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml)
[![pytest](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hyperion-sdk)
![GitHub Release](https://img.shields.io/github/v/release/tomasvotava/hyperion)

Hyperion organises data assets (a *data catalog*), validates them against Avro
schemas, abstracts the storage/queue/cache/secrets backends behind ports and
adapters, and gives you a framework for writing data **sources**. It runs the
same code on a laptop (local filesystem) or on AWS (S3/DynamoDB/SQS) — the
wiring is configuration, not code.

## Features

- **Data Catalog** — manage and organise data assets across backends.
- **Schema management** — validate and store Avro schema definitions per asset.
- **Ports & adapters** — swap S3/DynamoDB/SQS for local filesystem with config.
- **Source framework** — define sources that extract data into the catalog.
- **Caching** — in-memory, local file, and DynamoDB caching.
- **Async processing** — utilities for async operations and task queues.
- **CLI runner** — run sources standalone or in "Argo Workflow" mode.
- **Geo utilities** — Haversine math in the lite core; Google Maps via `[geo]`.
- **Asset collections** — a typed, declarative interface over groups of assets.

## Install

```bash
pip install 'hyperion-sdk[catalog]'
```

The default install is a slim **lite core**; heavy backends are opt-in extras.
See [Install Hyperion and pick extras](how-to/install-and-extras.md).

```python
from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import DataLakeAsset
from datetime import datetime, timezone

catalog = Catalog.from_config()
asset = DataLakeAsset(name="customer_data", date=datetime.now(timezone.utc), schema_version=1)
catalog.store_asset(asset, [{"id": 1, "name": "Customer 1"}])
```

## Documentation

This site follows the [Diataxis](https://diataxis.fr/) structure:

- **[Tutorials](tutorials/index.md)** — learning-oriented walkthroughs. Start
  with [your first DataLakeAsset](tutorials/first-datalake-asset.md).
- **[How-to guides](how-to/index.md)** — task-oriented recipes (extras,
  configuration, feature assets, caching, sources, geo, …).
- **[Reference](reference/migration-pre-1.0.md)** — the auto-generated API
  reference and the pre-1.0 migration guide.
- **[Explanation](explanation/index.md)** — the architecture, the ports/adapters
  design, and the lite-core + extras model.

!!! note "Upgrading from a pre-1.0 release?"
    Import paths moved and the `Catalog` constructor changed. See
    [Migrating from pre-1.0](reference/migration-pre-1.0.md).

## Project

- Source: <https://github.com/tomasvotava/hyperion>
- Changelog: <https://github.com/tomasvotava/hyperion/blob/master/CHANGELOG.md>
- Licensed under the MIT License.
