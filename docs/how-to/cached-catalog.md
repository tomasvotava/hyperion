# Use a cached Catalog

`Catalog.from_config()` wires storage from the environment but does **not**
attach a cache. To make repeated asset retrievals fast, inject a cache adapter
into the `Catalog` constructor explicitly.

## Catalog backed by local storage + a local file cache

```python
from pathlib import Path

from hyperion.catalog.catalog import Catalog
from hyperion.adapters.cache.filesystem import LocalFileCache
from hyperion.adapters.storage.filesystem import FilesystemStorage
from hyperion.adapters.schema_registry.local import LocalSchemaStore

cached_catalog = Catalog(
    storage=FilesystemStorage("/data/lake"),
    schema_store=LocalSchemaStore(Path("/data/schemas")),
    cache=LocalFileCache("catalog", default_ttl=3600, root_path=Path("/tmp/hyperion-cache")),
)

data = list(cached_catalog.retrieve_asset(asset))        # reads from storage
data_again = list(cached_catalog.retrieve_asset(asset))  # served from cache
```

## Add a cache to any storage backend

The same pattern works with any storage adapter — only `cache=` is required to
enable asset caching:

```python
catalog = Catalog(
    storage=FilesystemStorage("/data/lake"),
    cache=LocalFileCache("catalog", root_path=Path("/tmp/hyperion-cache")),
)

data1 = list(catalog.retrieve_asset(asset1))        # reads from storage
data1_again = list(catalog.retrieve_asset(asset1))  # served from cache
data2 = list(catalog.retrieve_asset(asset2))        # different asset → storage
```

Available cache adapters: `hyperion.adapters.cache.memory.InMemoryCache`,
`hyperion.adapters.cache.filesystem.LocalFileCache`, and
`hyperion.adapters.cache.dynamodb.DynamoDBCache` (requires the `[aws]` extra).

## General-purpose key-value cache

Independently of the catalog, you can use a cache directly for any data. Build
one from configuration via the `Cache` port:

```python
from hyperion.ports.cache import Cache

cache = Cache.from_config()

cache.set("my-key", "my-value")
print(cache.get("my-key"))   # "my-value"

if cache.hit("my-key"):
    print("Cache hit!")

cache.delete("my-key")
```

## See also

- [Configure backends via environment](configure-via-environment.md) for the
  `HYPERION_STORAGE_CACHE_*` variables.
- [Ports and adapters](../explanation/ports-and-adapters.md) for why caches are
  injected rather than configured on the `Catalog`.
