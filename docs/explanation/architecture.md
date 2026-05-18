# Architecture

Hyperion is organised as a set of **layers** following a ports-and-adapters
(hexagonal / DDD) design. The goal: the same orchestration code runs on a
laptop or on AWS, and which backend is used is a *configuration* decision made
in exactly one place.

## The layers

| Layer | Package | Responsibility | May depend on |
|---|---|---|---|
| **Domain** | `hyperion.domain` | Pure asset identities, messages, geo math. No I/O. | lite core only |
| **Ports** | `hyperion.ports` | Abstract contracts: `StoragePort`, `Cache`, `Queue`, `SchemaStore`, `SecretsManager`, `KeyValueStore`, `Geocoder`. | domain |
| **Adapters** | `hyperion.adapters` | Concrete backends (memory / filesystem / S3 / DynamoDB / SQS / Google Maps / avro). | domain, ports |
| **Application** | `hyperion.catalog.catalog`, `hyperion.application` | Orchestration — the `Catalog`. Depends on ports, never on adapters. | domain, ports |
| **Composition** | `hyperion.composition` | The only module that imports *both* ports and adapters and builds adapters from config. | everything |
| **Data** | `hyperion.data` | The pandera/polars validation stack — a leaf, behind `[data]`. | domain |

The lite core (`hyperion.log`, `hyperion.config`, `hyperion.dateutils`,
`hyperion.asyncutils`, `hyperion.typeutils`) sits below all of this and is
always installed.

## How a request flows

`Catalog.from_config()` asks the **composition root** for adapters; the
composition root reads `hyperion.config` and constructs the concrete `S3Storage`
/ `LocalSchemaStore` / cache / queue that the environment selects. The
`Catalog` then talks only to the **port** interfaces — it never imports an
adapter (the one documented carve-out is the avro serializer). Swapping
filesystem for S3 changes configuration, not the `Catalog` code.

```
config ──> composition root ──constructs──> adapters (S3 / filesystem / …)
                  │                              │ implement
                  └──> Catalog ── depends on ──> ports (StoragePort, Cache, …)
```

A **Source** sits on top: it extracts external data and yields assets that the
`Catalog` stores; messages about arrivals flow through the `Queue` port.

## Why this shape

- **Testability** — the application tier is exercised with in-memory adapters,
  no AWS.
- **Optionality** — heavy backends live in adapters and are gated by extras
  (see [Lite core and extras](extras-and-lite-core.md)); the lite core stays
  dependency-light.
- **One wiring point** — backend selection is centralised in the composition
  root instead of being scattered through call sites.

The precise "who may import whom" rules and how they are enforced are covered
in [Ports and adapters](ports-and-adapters.md).
