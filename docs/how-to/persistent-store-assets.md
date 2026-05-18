# Work with PersistentStoreAssets

A `PersistentStoreAsset` represents persistent data **without** time
partitioning — reference data, lookup tables, configuration, master data.

## Store and retrieve

```python
from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import PersistentStoreAsset

catalog = Catalog.from_config()

asset = PersistentStoreAsset(name="product_catalog", schema_version=1)

catalog.store_asset(asset, [
    {"id": "P001", "name": "Product 1", "category": "Electronics"},
    {"id": "P002", "name": "Product 2", "category": "Clothing"},
])

for product in catalog.retrieve_asset(asset):
    print(product)
```

Because there is no date partition, storing the same asset name + schema
version replaces the previous payload — that is the intended "current state"
semantics for reference data.

## See also

- [Register and use schemas](schemas.md) — persistent assets are validated
  against `persistent_store/{name}.v{version}.avro.json`.
- [Assets and the Catalog](../explanation/assets-and-catalog.md).
