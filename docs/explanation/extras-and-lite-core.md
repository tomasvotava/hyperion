# Lite core and extras

As of `1.0.0`, `pip install hyperion-sdk` installs a slim **lite core**. Heavy
backends are opt-in extras. This page explains the split and the reasoning.

## What is always installed

The lite core pulls only light, pure-Python-ish dependencies: `loguru`,
`pydantic`, `httpx`, `python-dateutil`, `python-dotenv`, `env-proxy`,
`cachetools`, `click`, `haversine`, `aws-lambda-typing`.

That is enough for the domain layer, the ports, configuration, logging, date
utilities, async utilities, Haversine geo math, and the in-memory / filesystem
adapters.

## The extras

| Extra | Pulls | Enables |
|---|---|---|
| `[aws]` | `boto3`, `aioboto3` | every `adapters/*/dynamodb`, `s3`, `sqs`, `aws_sm` (DynamoDB cache/keyval, S3 storage/schema, SQS queue, AWS Secrets Manager) |
| `[data]` | `polars`, `pandera`, `numpy` | `hyperion.data.*` (pandera↔polars typing, asset schemas, `SpatialKMeans`) |
| `[catalog]` | `fastavro` | `hyperion.adapters.serialization.avro` (avro-backed `Catalog`) |
| `[geo]` | `googlemaps` | `hyperion.adapters.geocoder.google` (`GoogleMaps`); Haversine math stays lite |
| `[snappy]` | `python-snappy` | compressed filesystem cache / DynamoDB keyval (graceful no-compression fallback when absent) |
| `[all]` | union of the above | parity with the pre-1.0 full install |

`[catalog]` no longer transitively requires `[aws]`: a `Catalog` backed by
local filesystem storage works with `[catalog]` alone.

## Why split it

- **Smaller, faster installs** — most consumers don't need `boto3` *and*
  `polars` *and* `googlemaps`. A source running in a constrained Lambda or a
  CLI tool only pays for what it uses.
- **Clear capability boundaries** — the extras map one-to-one onto adapter
  families, so "what do I need to install?" has a mechanical answer.
- **Architecturally enforced** — the lite-core promise (the core pulls no heavy
  third-party) is verified separately by the import-graph test and a
  no-extras smoke job, complementing the internal layering contract described
  in [Ports and adapters](ports-and-adapters.md).

## Picking extras

Install the capabilities you use, e.g.:

```bash
pip install 'hyperion-sdk[catalog,aws]'   # Catalog on S3
pip install 'hyperion-sdk[data]'          # feature models / asset collections
pip install 'hyperion-sdk[all]'           # everything (pre-1.0 parity)
```

See the task-focused [Install Hyperion and pick extras](../how-to/install-and-extras.md)
guide for combinations and version pinning.
