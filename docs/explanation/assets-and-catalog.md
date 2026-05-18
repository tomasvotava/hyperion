# Assets and the Catalog

## Assets

Assets are the fundamental units of data in Hyperion. Each asset represents a
dataset stored in a specific location with a defined schema. There are three
types:

### DataLakeAsset

- Raw, immutable data stored in a data lake.
- Time-partitioned by date; each partition has a schema version.
- Use for: raw API responses, event logs, source data preserved as-is.

### FeatureAsset

- Processed feature data with a **time resolution** (seconds → years).
- Can carry additional partition keys for finer-grained organisation.
- Use for: aggregated metrics, processed signals, ML features.

### PersistentStoreAsset

- Persistent data **without** time partitioning.
- Use for: reference data, lookup tables, configuration, master data.

## Schema management

Every asset has an associated Avro schema:

- **Schema store** — the `SchemaStore` port manages schemas in Avro format,
  backed by the local filesystem or S3.
- **Validation** — data is validated against its schema on storage.
- **Versioning** — assets carry a `schema_version` to support evolution.
- **Naming** — `{asset_type}/{asset_name}.v{version}.avro.json`.

A missing schema is an error, not a silent pass: storing or retrieving raises
until the schema is registered. See
[Register and use schemas](../how-to/schemas.md).

## The Catalog

The `Catalog` is the central orchestrator for asset storage and retrieval:

- **Storage routing** — maps each asset type to its storage backend (a single
  `StoragePort`, or a mapping per asset type).
- **Retrieval** — fetch assets by name, date, and schema version; iterate
  feature partitions over a date range.
- **Partitioning** — owns the partitioning logic per asset type, including
  repartitioning.
- **Notifications** — can emit messages (via the `Queue` port) when assets
  arrive.
- **Caching** — can use an injected cache for fast repeated retrieval (caching
  is opt-in; `from_config()` does not attach one — see
  [Use a cached Catalog](../how-to/cached-catalog.md)).

The `Catalog` depends only on the port interfaces; the concrete backends are
chosen by the composition root — see
[Ports and adapters](ports-and-adapters.md).
