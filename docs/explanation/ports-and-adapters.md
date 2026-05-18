# Ports and adapters

Hyperion uses dependency inversion: the application tier depends on **abstract
contracts** (ports), and **concrete backends** (adapters) implement them. The
binding between the two happens in exactly one place — the composition root.

## Ports

`hyperion.ports` holds the interfaces:

- `StoragePort` — object storage
- `Cache` — caching
- `KeyValueStore` — key-value persistence
- `Queue` — message queue
- `SecretsManager` — secret retrieval
- `SchemaStore` — Avro schema registry
- `Geocoder` — geocoding

Ports depend only on the domain and the lite core. They may *lazily* delegate
to the composition root for their `from_config()` factories, but they never
import an adapter.

## Adapters

`hyperion.adapters` holds the implementations, grouped by capability and by
backend, e.g. `adapters.storage.{memory,filesystem,s3}`,
`adapters.cache.{memory,filesystem,dynamodb}`, `adapters.queue.{…,sqs}`,
`adapters.geocoder.{static,google}`, `adapters.serialization.avro`. Heavy
backends are gated by [extras](extras-and-lite-core.md). An adapter may import
the domain and the ports it implements — but never the application tier or the
composition root.

## The composition root

`hyperion.composition` is the **only** module allowed to import both
`hyperion.ports` and `hyperion.adapters`. It reads `hyperion.config` and
constructs the adapter the environment selects. This is what makes
`Catalog.from_config()` (and `Cache.from_config()`, `GoogleMaps.from_config()`,
…) possible without the `Catalog` ever knowing S3 exists.

## The layering contract is enforced

These rules are not just convention — they are checked in CI by
[import-linter](https://import-linter.readthedocs.io/) (`.importlinter`):

- `domain` depends on nothing but the lite core.
- `ports` do not reach into adapters / application / data / catalog.
- `application` / `catalog.catalog` never import adapters (one documented
  carve-out: the avro serializer).
- `adapters` never import application / catalog / composition.
- `data` is a leaf — no live layer imports it.
- by the conjunction of the above, only `composition` imports both ports and
  adapters.

The deprecation shims (`hyperion.entities`, `hyperion.infrastructure.*`, the
`hyperion.catalog` namespace shim, the pandera/polars parts of
`hyperion.typeutils`) are intentionally *not* layer-constrained: they exist
only to redirect old import paths and are removed in 2.0 — see
[Migrating from pre-1.0](../reference/migration-pre-1.0.md).

## Why injection over configuration on the Catalog

Caches, queues and schema stores are *injected* into the `Catalog` constructor
rather than configured on it. This keeps the `Catalog` agnostic of any backend,
makes it trivial to test with in-memory adapters, and means new backends are
added as adapters without touching the orchestration code.
