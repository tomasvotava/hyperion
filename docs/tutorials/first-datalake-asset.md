# Your first DataLakeAsset

By the end of this tutorial you will have stored a data asset in a local
Hyperion catalog and read it back — with no AWS account and no environment
configuration. You will touch the four core pieces: a **schema**, a
**storage** adapter, a **schema store** adapter, and the **`Catalog`**.

## 1. Install

The catalog serialises assets as Avro, so install the `[catalog]` extra:

```bash
pip install 'hyperion-sdk[catalog]'
```

## 2. Lay out local directories

Hyperion can run entirely on local disk. Create two directories — one for the
data lake, one for schemas:

```bash
mkdir -p /tmp/hyperion/lake /tmp/hyperion/schemas/data_lake
```

## 3. Define a schema

Every asset is validated against an Avro schema. Schemas are named
`{asset_type}/{asset_name}.v{version}.avro.json`. Create
`/tmp/hyperion/schemas/data_lake/customer_data.v1.avro.json`:

```json
{
  "type": "record",
  "name": "CustomerData",
  "fields": [
    { "name": "id", "type": "int" },
    { "name": "name", "type": "string" }
  ]
}
```

## 4. Build a local Catalog

`Catalog.from_config()` reads the environment and wires S3. For this tutorial
construct the `Catalog` explicitly with the filesystem adapters instead:

```python
from pathlib import Path

from hyperion.catalog.catalog import Catalog
from hyperion.adapters.storage.filesystem import FilesystemStorage
from hyperion.adapters.schema_registry.local import LocalSchemaStore

catalog = Catalog(
    storage=FilesystemStorage("/tmp/hyperion/lake"),
    schema_store=LocalSchemaStore(Path("/tmp/hyperion/schemas")),
)
```

## 5. Store an asset

A `DataLakeAsset` is identified by a name, a date partition, and a schema
version. Store some records against the schema you defined:

```python
from datetime import datetime, timezone
from hyperion.domain.assets import DataLakeAsset

asset = DataLakeAsset(
    name="customer_data",
    date=datetime.now(timezone.utc),
    schema_version=1,
)

catalog.store_asset(asset, [
    {"id": 1, "name": "Customer 1"},
    {"id": 2, "name": "Customer 2"},
])
```

If the schema file is missing or the records don't match it, `store_asset`
raises — that is the schema validation doing its job.

## 6. Read it back

```python
for record in catalog.retrieve_asset(asset):
    print(record)
# {'id': 1, 'name': 'Customer 1'}
# {'id': 2, 'name': 'Customer 2'}
```

## What you learned

You wired a `Catalog` from a storage adapter and a schema store, validated
data against an Avro schema, and round-tripped an asset — all locally. The
same code runs on AWS by swapping the adapters (or using
`Catalog.from_config()`); see
[Configure backends via environment](../how-to/configure-via-environment.md).

Next: [build a custom Source](custom-source.md) to populate the catalog
automatically, or read
[Assets and the Catalog](../explanation/assets-and-catalog.md) for the concepts.
