# Install Hyperion and pick extras

## Install from PyPI

```bash
pip install hyperion-sdk
# or
poetry add hyperion-sdk
```

As of `1.0.0` the default install is a slim **lite core**. Heavy backends are
opt-in extras — install only the ones you use.

## Choose your extras

| Extra | Enables |
|---|---|
| `hyperion-sdk[aws]` | DynamoDB cache/keyval, S3 storage/schema, SQS queue, AWS Secrets Manager |
| `hyperion-sdk[data]` | pandera↔polars typing, asset schemas, `SpatialKMeans` |
| `hyperion-sdk[catalog]` | avro-backed `Catalog` (works with local filesystem storage alone) |
| `hyperion-sdk[geo]` | Google Maps geocoding (Haversine math stays in the lite core) |
| `hyperion-sdk[snappy]` | snappy-compressed filesystem cache / DynamoDB keyval |
| `hyperion-sdk[all]` | everything (parity with the pre-1.0 full install) |

Combine extras as needed, e.g. a Catalog on S3:

```bash
pip install 'hyperion-sdk[catalog,aws]'
```

The catalog examples elsewhere in these docs need at least
`pip install 'hyperion-sdk[catalog]'`; add `[aws]` if you store assets on S3.

## Pin your version

Hyperion follows [Semantic Versioning](https://semver.org/), so pin to a
compatible range to avoid breaking changes:

```bash
poetry add 'hyperion-sdk[catalog]@^1.0.0'
```

## Install from source

Hyperion uses [Poetry](https://python-poetry.org/):

```bash
git clone https://github.com/tomasvotava/hyperion.git
cd hyperion
poetry install
```

!!! note "Upgrading from a pre-1.0 release?"
    Import paths moved and the `Catalog` constructor changed. Follow
    [Migrating from pre-1.0](../reference/migration-pre-1.0.md) before
    upgrading. The rationale for the lite-core split is in
    [Lite core and extras](../explanation/extras-and-lite-core.md).
