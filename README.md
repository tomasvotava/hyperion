# Hyperion

A headless ETL / ELT / data pipeline and integration SDK for Python.

[![pre-commit](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pre-commit.yml)
[![pytest](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml/badge.svg?branch=master)](https://github.com/tomasvotava/hyperion/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/tomasvotava/hyperion/branch/master/graph/badge.svg?token=your-token)](https://codecov.io/gh/tomasvotava/hyperion)
![PyPI - License](https://img.shields.io/pypi/l/hyperion-sdk)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hyperion-sdk)
![GitHub Release](https://img.shields.io/github/v/release/tomasvotava/hyperion)

📚 **Documentation: <https://tomasvotava.github.io/hyperion/>**

Hyperion organises data assets (a *data catalog*), validates them against Avro
schemas, abstracts storage/queue/cache/secrets backends behind ports and
adapters, and gives you a framework for writing data **sources**. The same code
runs on a laptop (local filesystem) or on AWS (S3/DynamoDB/SQS) — the wiring is
configuration, not code.

## Features

- **Data Catalog** — manage and organise data assets across backends
- **Schema management** — validate and store Avro schemas per asset
- **Ports & adapters** — swap S3/DynamoDB/SQS for local filesystem via config
- **Source framework** — define sources that extract data into the catalog
- **Caching** — in-memory, local file, and DynamoDB caching
- **Asynchronous processing** — utilities for async operations and task queues
- **CLI runner** — run sources standalone or in "Argo Workflow" mode
- **Geo utilities** — Haversine math in the lite core; Google Maps via `[geo]`
- **Asset collections** — a typed, declarative interface over groups of assets

## Installation

```bash
pip install 'hyperion-sdk[catalog]'
# or
poetry add 'hyperion-sdk[catalog]'
```

As of `1.0.0` the default install is a slim **lite core**; heavy backends are
opt-in extras — install only the ones you use:

| Extra | Enables |
|---|---|
| `hyperion-sdk[aws]` | DynamoDB cache/keyval, S3 storage/schema, SQS queue, AWS Secrets Manager |
| `hyperion-sdk[data]` | pandera↔polars typing, asset schemas, `SpatialKMeans` |
| `hyperion-sdk[catalog]` | avro-backed `Catalog` (works with local filesystem storage alone) |
| `hyperion-sdk[geo]` | Google Maps geocoding (Haversine math stays lite) |
| `hyperion-sdk[snappy]` | snappy-compressed filesystem cache / DynamoDB keyval |
| `hyperion-sdk[all]` | everything (parity with the pre-1.0 full install) |

We follow [Semantic Versioning](https://semver.org/) — pin with a selector such
as `^1.0.0`. **Upgrading from a pre-1.0 release?** Import paths moved and the
`Catalog` constructor changed — see the
[migration guide](https://tomasvotava.github.io/hyperion/reference/migration-pre-1.0/)
(also pinned at the top of [`CHANGELOG.md`](./CHANGELOG.md)).

## Quickstart

```python
from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import DataLakeAsset
from datetime import datetime, timezone

catalog = Catalog.from_config()
asset = DataLakeAsset(name="customer_data", date=datetime.now(timezone.utc), schema_version=1)
catalog.store_asset(asset, [{"id": 1, "name": "Customer 1"}])
```

A runnable, no-AWS walkthrough is in the
[first DataLakeAsset tutorial](https://tomasvotava.github.io/hyperion/tutorials/first-datalake-asset/).

## Documentation

Full documentation is published at **<https://tomasvotava.github.io/hyperion/>**
and follows the [Diataxis](https://diataxis.fr/) structure:

- [Tutorials](https://tomasvotava.github.io/hyperion/tutorials/) — learning-oriented walkthroughs
- [How-to guides](https://tomasvotava.github.io/hyperion/how-to/) — task-oriented recipes
- [Reference](https://tomasvotava.github.io/hyperion/reference/migration-pre-1.0/) — auto-generated API reference + migration guide
- [Explanation](https://tomasvotava.github.io/hyperion/explanation/) — architecture and design

## Development

Hyperion uses [Poetry](https://python-poetry.org/):

```bash
git clone https://github.com/tomasvotava/hyperion.git
cd hyperion
poetry install
poetry run pre-commit install

poetry run pytest                  # run tests
poetry run pre-commit run -a       # ruff + mypy + hooks
poetry install --with docs         # docs toolchain
poetry run mkdocs serve            # preview the docs site locally
```

The project uses [ruff](https://github.com/astral-sh/ruff) (linting),
[mypy](https://mypy.readthedocs.io/) (type checking), and
[commitizen](https://github.com/commitizen-tools/commitizen) (conventional
commits). See the
[architecture explanation](https://tomasvotava.github.io/hyperion/explanation/architecture/)
for the layered design.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License — see the LICENSE file for
details.
