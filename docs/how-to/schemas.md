# Register and use schemas

Every asset is validated against an Avro schema during storage and retrieval.
Schemas are named with the pattern:

```
{asset_type}/{asset_name}.v{version}.avro.json
```

where `asset_type` is `data_lake`, `feature`, or `persistent_store`. If a
schema is missing, `store_asset`/`retrieve_asset` raises until you provide it.

## Define and register a schema

```python
import json
from pathlib import Path

schema = {
    "type": "record",
    "name": "CustomerData",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
}

# Local schema store
schema_path = Path("schemas/data_lake/customer_data.v1.avro.json")
schema_path.parent.mkdir(parents=True, exist_ok=True)
schema_path.write_text(json.dumps(schema))
```

## Register a schema on S3

When using `S3SchemaStore` (the `[aws]` extra), upload the schema under the
same key layout:

```python
import boto3, json

boto3.client("s3").put_object(
    Bucket="my-schema-bucket",
    Key="data_lake/customer_data.v1.avro.json",
    Body=json.dumps(schema),
)
```

## Schema evolution

Assets carry a `schema_version`. To evolve a schema, register a new file with
an incremented version (`...v2.avro.json`) and bump `schema_version` on the
asset; old partitions keep resolving against their original version.

## See also

- [Your first DataLakeAsset](../tutorials/first-datalake-asset.md) walks
  through a schema end to end.
- API: [`hyperion.adapters.schema_registry.local`](../reference/adapters/schema_registry/local.md)
  and [`...schema_registry.s3`](../reference/adapters/schema_registry/s3.md).
