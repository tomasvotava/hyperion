# Migrating from pre-1.0

`1.0.0` finishes the ports/adapters (DDD) layout. It is a **structural**
refactor — no features added, no behaviour changed (one bug fixed). Every
relocated symbol stays importable from its old path for the whole 1.x line
behind a `DeprecationWarning`; the old paths are removed in 2.0. The single
non-additive break is the `Catalog` constructor, mitigated by
`Catalog.from_config()`.

!!! info "The canonical migration guide lives in the changelog"
    The authoritative, always-current upgrade guide is pinned at the top of
    [`CHANGELOG.md`](https://github.com/tomasvotava/hyperion/blob/master/CHANGELOG.md).
    It is regenerated on every release, so it is the single source of truth.
    This page summarises it; follow the changelog for the exact symbol table.

## What changed, in three parts

### 1. Packaging: lite core + opt-in extras

`pip install hyperion-sdk` is now a slim lite core. Heavy backends are opt-in
extras. A consumer that used `Catalog`/avro now needs `hyperion-sdk[catalog]`;
DynamoDB/S3/SQS needs `[aws]`; the pandera/polars helpers need `[data]`; Google
geocoding needs `[geo]`; `[all]` reproduces the pre-1.0 install exactly. See
[Install Hyperion and pick extras](../how-to/install-and-extras.md) and the
rationale in [Lite core and extras](../explanation/extras-and-lite-core.md).

### 2. Import paths moved (deprecated, removed in 2.0)

58 symbols across 12 modules moved from `hyperion.infrastructure.*`,
`hyperion.entities.*`, `hyperion.catalog`/`hyperion.catalog.schema`, and the
pandera/polars parts of `hyperion.typeutils` into the new
`hyperion.ports.*` / `hyperion.adapters.*` / `hyperion.domain.*` /
`hyperion.data.*` layout. **Only the import module changes — symbol names are
unchanged.** Old paths keep working through all of 1.x but emit a
`DeprecationWarning` naming the new path. Run your test suite with deprecation
warnings visible to surface every call site mechanically, and consult the
[changelog table](https://github.com/tomasvotava/hyperion/blob/master/CHANGELOG.md)
for the exact old→new mapping.

The lite core (`hyperion.log`, `hyperion.config`, `hyperion.dateutils`,
`hyperion.asyncutils`) did **not** move, and `hyperion.typeutils` keeps its
stdlib helpers.

### 3. The `Catalog` constructor (the one hard break)

`Catalog.__init__` is the only non-additive break — no compat shim. The old
bucket/prefix keyword arguments, `StoreBucketConfig`, `Catalog.s3_client`, and
`get_store_config()` are removed.

```python
from hyperion.ports.storage import StoragePort

Catalog(
    storage=...,        # a StoragePort, or Mapping[AssetType, StoragePort]
    queue=None,
    cache=None,
    schema_store=None,
    serializer=None,    # AvroSerializer; a fresh one by default
)
```

A single `StoragePort` serves every asset type; pass a mapping keyed by
`"data_lake"` / `"feature"` / `"persistent_store"` to route each type to a
different backend.

**Escape hatch:** `Catalog.from_config()` is unchanged in name and intent — it
rebuilds the per-bucket S3 layout from the environment. Consumers that only ever
used `Catalog.from_config()` need no constructor change.

**Bug fixed (behaviour change, deliberate):** pre-1.0 `Catalog.from_config()`
wired the feature store with the *data-lake* prefix. It now correctly uses the
feature-store prefix. Where the two prefixes differ, feature-store asset keys
resolve to a different (correct) path after upgrading — review any tooling that
depended on the old, wrong path.

## Migration checklist

1. **Relax the version pin** to admit `1.x` (e.g. widen `>=0.15.0,<1`).
2. **Request the extras you use** — the default install is now lite.
3. **Rewrite moved imports** using the changelog table (names unchanged).
4. **Check direct `Catalog(...)` construction** — switch to
   `Catalog.from_config()` or the new `storage=` constructor.
5. **Account for the `from_config()` prefix fix** if your data-lake and
   feature-store prefixes differ.
