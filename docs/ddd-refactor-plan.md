# hyperion-sdk: DDD refactor — task description

## Context

`hyperion-sdk` (at `/home/rayt/repos/personal/hyperion`) was shipped under time pressure with a partially-realized DDD layout. The intent was visible — `Cache`, `Queue`, `SchemaStore`, `SecretsManager` are abstract base classes with multiple implementations — but the pattern was never carried all the way through. As a result:

- `Catalog` (the package's flagship abstraction) cannot run without `boto3`, even though it was originally meant to work with local filesystem storage.
- `geo.Location` reaches for a global `Cache` singleton via class-level state, so the geo subpackage transitively depends on whatever cache implementation is configured (typically DynamoDB), forcing AWS deps on consumers that just want to compute distances.
- `PersistentCache` and `GoogleMaps` create a circular knot: cache uses Catalog to persist itself; Catalog uses cache to speed up retrieval.
- `typeutils.py` and `entities/catalog.py` import `polars`/`pandera` at module level, so any consumer that touches a type-utility helper transitively loads the entire data-validation stack.
- The default install (`pip install hyperion-sdk`) pulls `aioboto3, boto3, fastavro, numpy, polars, pandera, googlemaps, python-snappy` whether or not the consumer needs them.

This task is a **pure structural refactor**. The goal is to finish the DDD layout (proper layering + dependency inversion), so that `pip install hyperion-sdk` gives a slim lite core, and consumers opt into `[aws]`, `[data]`, `[catalog]`, `[geo]` only where they actually use those capabilities.

**No new features. No API additions. No behaviour changes.** Anything that is not a layering, naming, or dependency-direction fix is out of scope.

The package author/sole maintainer requested this and is OK with tough love. There is no schedule pressure.

---

## What the current layout gets right (preserve)

- `Cache` (`hyperion/infrastructure/cache.py:67`), `Queue` (`hyperion/infrastructure/message_queue.py:110`), `SchemaStore` (`hyperion/catalog/schema.py:20`), `SecretsManager` (`hyperion/infrastructure/secretsmanager.py:16`), `KeyValueStore` (`hyperion/infrastructure/keyval.py:25`) are already abstract bases with multiple concrete impls (DynamoDB/Local/InMemory pattern repeated four times).
- `hyperion/__init__.py` is empty — no eager loading at the top level.
- `log.py`, `dateutils.py`, `asyncutils.py`, `config.py` are dependency-light and self-contained.
- Test suite already uses `moto_server` (`tests/conftest.py:29`) for AWS mocking and parametrizes tests over cache backends (`tests/infrastructure/test_cache.py`) — the pattern to extend, not invent.

These pieces stay where they are conceptually; they just move to clearer locations and shed their accidental couplings.

---

## Where it falls short (the actual targets of the refactor)

Each item below is a concrete pain point with a file:line citation, paired with the fix it implies.

### F1. `Catalog` is locked to S3 (broken promise: local-path support)
`hyperion/catalog/catalog.py:196` constructs and uses `S3Client` directly. There is no `StoragePort` abstraction. Result: there is no way to instantiate a `Catalog` against a local directory, even though that was an original design intent.
**Fix:** introduce a `StoragePort` (Protocol with `put`/`get`/`iter_keys`/`get_attributes`/`exists`/`delete`), make `Catalog` depend on it, ship `S3Storage` (boto3, behind `[aws]`), `FilesystemStorage` (stdlib), and `MemoryStorage` (testing).

### F2. `geo` transitively depends on `[aws]`
`hyperion/infrastructure/geo/location.py:14` does `from hyperion.infrastructure.cache import Cache`, and `Location._cache` is a *class variable* lazy-initialized from global config (`_get_cache()`). Anyone using `Location.get_distance` triggers cache resolution, which in production resolves to `DynamoDBCache` and pulls boto3.
**Fix:** `Location` should not own a cache. Either (a) make distance caching the caller's responsibility — drop the `_cache` class variable, let callers wrap with `functools.lru_cache` or pass a cache-decorated wrapper, OR (b) accept an explicit `Cache | None` argument (default `None` = no caching). Option (a) is cleaner and matches the "domain object has no infrastructure" principle.

### F3. `PersistentCache` is a circular knot
`hyperion/infrastructure/cache.py:475` defines `PersistentCache` — an in-memory dict that loads/saves via `Catalog.retrieve_asset` / `Catalog.store_asset`. So cache depends on catalog, while catalog depends on cache.
**Fix:** `PersistentCache` is not a cache; it's a *catalog-backed dict adapter*. Move it out of `infrastructure/cache.py` entirely. Either: rename to `CatalogBackedStore` and place under the application layer (it's a use-case, not a primitive), or — since its sole real consumer is `GoogleMaps` (F4) — delete it once F4 is resolved and replace with a plain `KeyValueStore` injected into `GoogleMaps`.

### F4. `GoogleMaps` depends on `Catalog` via `PersistentCache`
`hyperion/infrastructure/geo/gmaps.py:34` wires its geocode cache through `PersistentCache(GEOCodeCache asset)`. That means `GoogleMaps` (an infrastructure adapter) reaches up into the application layer (`Catalog`) just to persist a key-value mapping. Wrong direction.
**Fix:** `GoogleMaps` accepts a `KeyValueStore` in its constructor (already an existing abstraction at `hyperion/infrastructure/keyval.py:25`). The composition root wires it with `InMemoryStore`, `DynamoDBStore`, or a filesystem-backed store — caller's choice.

### F5. `typeutils` mixes stdlib helpers and the data stack at module level
`hyperion/typeutils.py:7-10` imports `pandera`, `pandera.engines`, `pandera.engines.polars_engine`, `polars`. Stdlib helpers like `assert_type`, `dataclass_asdict`, `is_typed_dict_instance` live in the same file. Any consumer importing the stdlib helpers pays the full data-validation cost.
**Fix:** split into `hyperion/typeutils.py` (stdlib-only) and `hyperion/data/typeutils.py` (pandera/polars mapping, behind `[data]`).

### F6. `entities/catalog.py` puts data-validation framework into domain models
`hyperion/entities/catalog.py:6-8` imports `pandera.polars` and `polars` at module level — but the asset *identity* (name, schema version, partition keys, paths) doesn't need pandera at all. Pandera is only relevant when validating/loading dataframes.
**Fix:** split `DataLakeAsset` / `FeatureAsset` / `PersistentStoreAsset` into two layers: a pure-pydantic identity layer (`hyperion/domain/assets.py`, no pandera/polars) and a data-validation layer (`hyperion/data/asset_schemas.py`, requires `[data]`). The lite consumer only needs identity to construct paths and message envelopes.

### F7. Misleading directory name: `infrastructure/`
`hyperion/infrastructure/` mixes abstractions (`Cache`, `Queue`, `KeyValueStore`, `SecretsManager`) with concrete implementations (`S3Client`, `DynamoDBCache`, `SQSQueue`, `FileQueue`, `googlemaps.GoogleMaps`, `httputils`). DDD-wise the abstractions are *ports* and the concretes are *adapters* — these belong in different layers.
**Fix:** move abstractions to `hyperion/ports/` and concrete implementations to `hyperion/adapters/<port>/<backend>.py`. See target layout below.

### F8. `hyperion/catalog/__init__.py` eagerly loads heavy code on namespace touch
`hyperion/catalog/__init__.py:1-2` does `from .catalog import AssetNotFoundError, Catalog` and `from .schema import SchemaStore`. Just touching the `hyperion.catalog` namespace pulls `fastavro`, `boto3`, and the data stack into memory — even if a consumer only wants to type-hint with `AssetNotFoundError`.
**Fix:** either empty the `__init__.py` and require explicit submodule imports, or use PEP 562 `__getattr__` for lazy loading. Prefer empty — explicit is better than magic, and the migration window includes a deprecation note pointing consumers at the new explicit path.

### F9. Singletons / class-level mutable state
- `Location._cache` (`location.py`) — class variable
- `GoogleMaps._instance` (`gmaps.py:81-ish`) — singleton via `from_config()`
- `Cache.from_config()` (`cache.py`) — singleton selecting impl from env
- `Queue.from_config()`, `SchemaStore.from_config()`, `SecretsManager.from_config()` — same pattern

**Fix:** keep the `from_config()` factories (they're useful at the composition root), but stop relying on them inside library code. Adapters should be constructed and injected, not pulled from a global singleton from inside another module. This is the largest behavioural-feeling change, but it doesn't change observable behaviour — it just moves construction from "anywhere in the call graph" to "the composition root, once".

---

## Target architecture

```
hyperion/
  __init__.py                         # empty (no eager re-exports)
  __version__.py

  # ---- Lite core (default install) ----
  log.py                              # loguru-based; no changes
  dateutils.py                        # no changes
  asyncutils.py                       # no changes
  config.py                           # no changes
  typeutils.py                        # stdlib helpers only (F5)

  # ---- Domain layer (no infrastructure, no I/O) ----
  domain/
    __init__.py
    assets.py                         # pure pydantic Asset identities (F6) — name, version,
                                      # partition keys, S3 path helpers. No polars/pandera.
    geo.py                            # Location, NamedLocation — pure data + math (haversine).
                                      # No cache reference (F2). No singletons (F9).
    messages.py                       # message dataclasses (move from message_queue.py:38)

  # ---- Ports layer (interfaces, dependency inversion) ----
  ports/
    __init__.py
    storage.py                        # NEW (F1): StoragePort Protocol
    cache.py                          # MOVED from infrastructure/cache.py:67 (abstract Cache)
    keyval.py                         # MOVED from infrastructure/keyval.py:25 (abstract KVS)
    queue.py                          # MOVED from infrastructure/message_queue.py:110 (abstract Queue)
    secrets.py                        # MOVED from infrastructure/secretsmanager.py:16
    schema_registry.py                # MOVED from catalog/schema.py:20 (abstract SchemaStore)
    geocoder.py                       # NEW (extract from GoogleMaps): Geocoder Protocol
                                      # methods: geocode, reverse_geocode, get_altitude

  # ---- Application layer (orchestration, depends only on ports) ----
  application/
    __init__.py
    catalog.py                        # Catalog: takes (storage, cache?, queue?, schema_registry)
                                      # in constructor (F1). No boto3 import.
    repartitioner.py                  # AssetRepartitioner (depends on Catalog port surface)
    schema_resolver.py                # parses file:// vs s3:// schemes, delegates to adapter

  # ---- Adapters layer (concrete impls, one subpackage per port, extras-gated) ----
  adapters/
    __init__.py
    storage/
      __init__.py
      memory.py                       # MemoryStorage — dict-of-bytes, for tests (lite)
      filesystem.py                   # FilesystemStorage — pathlib-based (lite). Restores
                                      # original "Catalog with local path" promise.
      s3.py                           # S3Storage — boto3 + aioboto3 [aws]
    cache/
      __init__.py
      memory.py                       # InMemoryCache (moved from cache.py:276) (lite)
      filesystem.py                   # LocalFileCache (moved from cache.py:316) — snappy is now [snappy]
      dynamodb.py                     # DynamoDBCache (moved from cache.py:570) [aws]
    keyval/
      __init__.py
      memory.py                       # InMemoryStore (moved from keyval.py:250) (lite)
      filesystem.py                   # NEW lite adapter (json/sqlite-backed)
      dynamodb.py                     # DynamoDBStore (moved from keyval.py:197) [aws]
    queue/
      __init__.py
      memory.py                       # InMemoryQueue (moved from message_queue.py:169) (lite)
      filesystem.py                   # FileQueue (moved from message_queue.py:210) (lite)
      sqs.py                          # SQSQueue (moved from message_queue.py:185) [aws]
    secrets/
      __init__.py
      env.py                          # NEW lite adapter (read from os.environ)
      dummy.py                        # DummySecretsManager (moved) (lite)
      aws_sm.py                       # AWSSecretsManager (moved) [aws]
    schema_registry/
      __init__.py
      local.py                        # LocalSchemaStore (moved from catalog/schema.py:101) (lite)
      s3.py                           # S3SchemaStore (moved from catalog/schema.py:141) [aws]
    serialization/                    # NEW (extract from Catalog internals)
      __init__.py
      avro.py                         # fastavro reader/writer (encapsulates fastavro use) [catalog]
    geocoder/
      __init__.py
      static.py                       # NEW lite adapter (static lookup, for tests / offline)
      google.py                       # GoogleMaps geocoder (moved, simplified) [geo]
    http/
      __init__.py
      proxy.py                        # MOVED from infrastructure/httputils.py (lite)

  # ---- Data adapters (heavy validation, [data] extra) ----
  data/
    __init__.py
    typeutils.py                      # pandera↔polars mapping (split from typeutils.py) (F5) [data]
    asset_schemas.py                  # pandera schemas for assets (split from entities/catalog.py) (F6) [data]
    validators.py                     # validation helpers [data]

  # ---- Sources (CLI for data source runners) ----
  sources/
    __init__.py
    base.py                           # Source base (already light) — update imports
    cli.py                            # click CLI (already light)

  # ---- Composition / factories ----
  composition.py                      # NEW: from_config() factories that wire ports → adapters
                                      # based on env vars. Replaces scattered .from_config()
                                      # singletons (F9). Single source of truth.
```

### Layering rules (enforced by an import-linter rule + reviews)

1. `domain/` may not import from anything except stdlib, pydantic, dateutil, `hyperion.{log,dateutils,typeutils,asyncutils,config}`.
2. `ports/` may import from `domain/` and lite core; no `adapters/`, no third-party heavy deps.
3. `application/` may import from `domain/` and `ports/`; never from `adapters/` directly.
4. `adapters/` may import from `domain/`, `ports/`, and their own backend's third-party deps. Never from `application/`.
5. `data/` is an adapter package for the validation stack; depends on `domain/`, may not be imported from anywhere outside `data/` and tests.
6. `composition.py` is the only place allowed to import from both `ports/` and `adapters/` and construct adapters from config.

Adding `import-linter` to dev deps and a `.importlinter` config makes this enforceable in CI; a single contract file (~40 lines) covers all six rules.

---

## Extras model (derives directly from adapter directories)

Default `pip install hyperion-sdk` (lite core):
`loguru, pydantic, httpx, python-dateutil, python-dotenv, env-proxy, cachetools, click, haversine, aws-lambda-typing`

| Extra | Adds | Enables |
|---|---|---|
| `[aws]` | `boto3, aioboto3` | All `adapters/*/dynamodb.py`, `s3.py`, `sqs.py`, `aws_sm.py` |
| `[data]` | `polars, pandera, numpy` | `hyperion/data/*` |
| `[catalog]` | `fastavro` | `adapters/serialization/avro.py` (combine with `[aws]` for S3-backed catalog, or use alone with filesystem storage) |
| `[geo]` | `googlemaps` | `adapters/geocoder/google.py` (Haversine is in lite for `domain/geo.py` math) |
| `[snappy]` | `python-snappy` | Compressed `adapters/cache/filesystem.py` and `adapters/keyval/dynamodb.py` (graceful no-compression fallback when absent) |
| `[all]` | union | Parity with today's full install |

Note: `[catalog]` no longer transitively requires `[aws]`. A consumer can build a `Catalog(storage=FilesystemStorage("/data"), schema_registry=LocalSchemaStore("/schemas"))` with only `[catalog]`. This is the restored promise.

---

## Refactor sequence (incremental — tests stay green throughout)

Each step is independently mergeable. The order is chosen so the test suite passes after every step.

### Step 1: extract ports (no behavioural change)
- Create `hyperion/ports/` with one file per abstraction
- *Move* the abstract base classes from `infrastructure/cache.py`, `infrastructure/message_queue.py`, `infrastructure/keyval.py`, `infrastructure/secretsmanager.py`, `catalog/schema.py` into the corresponding `ports/*.py`
- Update old locations to re-export from `ports/` (compat shims, deprecation warning at import)
- Add `StoragePort` (new — see Step 4) as an empty Protocol stub for now
- Run tests — all green (only imports moved)

### Step 2: split typeutils and entities (F5, F6)
- Create `hyperion/data/typeutils.py` containing the pandera/polars half of `typeutils.py`
- Slim `hyperion/typeutils.py` to stdlib-only helpers
- Create `hyperion/domain/assets.py` with pydantic-only Asset identity (extract from `entities/catalog.py`)
- Create `hyperion/data/asset_schemas.py` with pandera schemas (keep the heavy half)
- Re-export from old `entities.catalog` for compat
- Add an import-time smoke test that `import hyperion.domain.assets` works without `polars` installed

### Step 3: extract domain geo (F2)
- Create `hyperion/domain/geo.py` with `Location`, `NamedLocation`, distance methods (haversine), `SpatialKMeans` (numpy is in lite, only used optionally — verify if numpy can be made `[data]`-gated or kept lite)
- Remove `Location._cache` class variable. `Location.get_distance` no longer caches; if callers want caching they wrap with `functools.lru_cache` or use a higher-level service
- Re-export from `infrastructure/geo/location.py` for compat
- Note: if removing the cache is a measurable perf regression for known callers, add an `application/geo_distance_service.py` that wraps `domain.geo.Location.get_distance` and accepts an injected `Cache`

### Step 4: introduce StoragePort and FilesystemStorage / S3Storage (F1)
- Define `StoragePort` Protocol in `ports/storage.py` with the methods Catalog actually needs (see Section "StoragePort signature" below)
- Create `adapters/storage/memory.py` (dict-of-bytes), `adapters/storage/filesystem.py` (pathlib), `adapters/storage/s3.py` (move `S3Client` body here, simplify constructor)
- Catalog still uses `S3Client` directly — leave it for now
- Tests: add unit tests for each storage adapter using the existing fixture pattern (parametrize over adapters)

### Step 5: invert Catalog's storage dependency (the core of F1)
- Change `Catalog.__init__` to accept `storage: StoragePort` instead of `s3_client: S3Client` and bucket parameters. Bucket names become part of `S3Storage` construction.
- Extract the avro read/write code from `Catalog` into `adapters/serialization/avro.py` with an `AvroSerializer` class (or two functions). `Catalog` now calls `self._serializer.encode(data)` / `decode(blob)`.
- Inject `serializer: AvroSerializer` into Catalog (or default-construct it if `[catalog]` is installed)
- Run integration tests: parametrize Catalog over `S3Storage(via moto)` and `FilesystemStorage(tmp_path)`. The local-storage Catalog tests are *new* and prove the restored promise.

### Step 6: move concrete adapters to `adapters/*` (F7)
- Mechanical file moves for cache, keyval, queue, secrets, schema_registry impls
- Each backend goes to `adapters/<port>/<backend>.py`
- Old `infrastructure/*` files become compat re-exports with deprecation warnings
- The `infrastructure/` directory becomes empty (or holds the deprecation shims only) — eventually deleted in 2.x

### Step 7: rip out the PersistentCache knot (F3, F4)
- Delete `PersistentCache` (`infrastructure/cache.py:475`)
- Change `GoogleMaps.__init__` to accept `keyval: KeyValueStore` (use existing port). Default in `composition.py` is `InMemoryStore`; users wire `DynamoDBStore` or a filesystem-backed store if they want persistence
- `GoogleMaps` is no longer a context manager (its `__enter__`/`__exit__` was just the PersistentCache flush) — but keep the `__enter__`/`__exit__` returning `self` for compat with old `with GoogleMaps() as g:` callers
- Move `GoogleMaps` to `adapters/geocoder/google.py` and have it implement a new `Geocoder` Protocol in `ports/geocoder.py`

### Step 8: introduce `composition.py` and replace scattered `from_config()` (F9)
- Add `hyperion/composition.py` with module-level functions: `default_storage()`, `default_cache()`, `default_queue()`, etc. — each reads env config and returns the appropriate adapter
- Keep `Cache.from_config()` etc. on the port classes but have them delegate to `composition` module — single source of truth
- Document at the top of `composition.py`: "this is the only place that knows about both ports and adapters"

### Step 9: lazy-load catalog namespace (F8)
- Empty `hyperion/catalog/__init__.py` — leave a comment pointing to the new explicit paths
- Add deprecation notice in the docstring / a `__getattr__` that warns on the legacy import path for one release cycle

### Step 10: convert `pyproject.toml` to extras model
- Move heavy deps from `[tool.poetry.dependencies]` to `[project.optional-dependencies]` (or poetry equivalent)
- Default deps: lite core only
- Define the five extras (`aws`, `data`, `catalog`, `geo`, `snappy`) + `all`
- Add the import-linter contract config
- Add a CI job: `pip install hyperion-sdk` (no extras), then `python -c "import hyperion.log, hyperion.dateutils, hyperion.config, hyperion.asyncutils, hyperion.domain.assets, hyperion.domain.geo, hyperion.ports.storage"` — succeeds and `pip list` does not contain `boto3`, `polars`, `fastavro`, `numpy`, `pandera`, `googlemaps`
- Add a second CI job per extra that imports the matching adapter package and verifies the dep is present

### Step 11: release as `hyperion-sdk` 1.0.0
- Update CHANGELOG with the layering migration table (`old.path → new.path`)
- Mark deprecated re-export shims with `DeprecationWarning` and a target removal version (1.x → remove in 2.0)
- Tag, publish

---

## Port signatures (cheat-sheet for Step 1 + Step 4)

```python
# ports/storage.py — NEW
class StoragePort(Protocol):
    def put(self, key: str, data: bytes | IO[bytes]) -> None: ...
    async def put_async(self, key: str, data: bytes | IO[bytes]) -> None: ...
    def get(self, key: str) -> bytes: ...
    def open(self, key: str) -> ContextManager[IO[bytes]]: ...   # streaming read
    def iter_keys(self, prefix: str) -> Iterator[str]: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...
    def get_attributes(self, key: str) -> ObjectAttributes: ...  # etag, size, last_modified
```

The other ports already exist as ABCs; they just move from `infrastructure/*` to `ports/*` and switch from `ABC` to `Protocol` if we want structural typing (recommended — avoids forced inheritance for third-party adapters, but check compat with current `isinstance` uses).

The `Geocoder` port is new:

```python
# ports/geocoder.py — NEW
class Geocoder(Protocol):
    def geocode(self, address: str) -> Location: ...
    def reverse_geocode(self, location: Location, language: str | None = None) -> NamedLocation: ...
    def get_altitude(self, location: Location) -> float: ...
```

---

## Backward compatibility strategy

1. **One release of compat shims.** Every moved symbol stays importable from its old location for the entire 1.x line. Each shim emits a `DeprecationWarning` pointing at the new path.
2. **CHANGELOG migration table** lists every renamed import for fast `sed` migration in consumers (one shipping consumer today: zephlib).
3. **Catalog constructor signature changes** — this is the one non-additive break. Old: `Catalog(data_lake_bucket="…", feature_store_bucket="…", …)`. New: `Catalog(storage=S3Storage(data_lake_bucket=…, …))`. Keep a `Catalog.from_config()` factory that builds the old default for existing callers; release notes explicitly call this out.
4. **Removal in 2.0.** Compat shims, `from_config()` defaults, and old import paths disappear in `hyperion-sdk` 2.0. Cadence: at least one minor release of warnings before any removal.

---

## Testing strategy

- **Memory adapters become first-class.** Every port gets a memory adapter (`adapters/*/memory.py`). Unit tests use memory adapters; integration tests use moto. The existing parametrize-over-backends pattern in `tests/infrastructure/test_cache.py` extends naturally to other ports.
- **The Catalog test suite gains a new fixture matrix** — same tests run against `S3Storage(moto)` and `FilesystemStorage(tmp_path)`. Any test that requires features S3 has but filesystem lacks (or vice versa) is explicitly marked.
- **New import-linter contract test** — runs in CI, asserts the layering rules in Section "Layering rules".
- **No-extras smoke test job** — proves the lite core actually installs lean (see Step 10).
- **Per-extra smoke test jobs** — one job per extra: install only that extra, import the adapter modules it enables, verify they work end-to-end with a minimal scenario.

---

## Out of scope (explicit)

To keep this refactor honest:

- **No new ports.** Only ones that are missing for *existing* functionality (StoragePort, Geocoder). No speculative additions.
- **No new adapters beyond what the layout requires.** Filesystem adapters for storage/keyval/queue/secrets are added because the layout demands a lite-tier impl for each port. No "Redis cache" or "Postgres keyval" — those are features.
- **No API additions.** No new methods on Catalog, no new convenience functions, no new asset types.
- **No behaviour changes.** Existing `from_config()` defaults select the same backend they select today. Geo distance caching is removed (F2) only if benchmarks confirm no meaningful regression for known callers; otherwise the application-layer wrapper from Step 3 is used.
- **No build-backend change.** Stay on `poetry-core`. (uv migration is a separate decision.)
- **No CI overhaul.** Just add the import-linter job and the install-smoke-test jobs; leave existing pytest/release flow alone.
- **No dependency-version bumps unless required** by the refactor itself.

---

## Verification checklist (definition of done)

- [ ] `pip install hyperion-sdk` (no extras) installs without `boto3, polars, pandera, numpy, fastavro, googlemaps, python-snappy`
- [ ] `python -c "import hyperion.log, hyperion.dateutils, hyperion.config, hyperion.asyncutils, hyperion.domain.assets, hyperion.domain.geo, hyperion.ports.storage, hyperion.ports.cache"` succeeds in that environment
- [ ] `pip install 'hyperion-sdk[catalog]'` (no `[aws]`) lets a user build `Catalog(storage=FilesystemStorage("/tmp/cat"), schema_registry=LocalSchemaStore("/tmp/schemas"))` and store/retrieve an asset round-trip from local disk
- [ ] `pip install 'hyperion-sdk[geo]'` (no `[aws]`) lets a user use `GoogleMaps(keyval=InMemoryStore(), api_key=…)` without pulling boto3
- [ ] `import-linter` job is green — all six layering rules hold
- [ ] Full extras suite (`pip install 'hyperion-sdk[all]'`) — entire existing test suite passes unchanged
- [ ] Every legacy import path emits a `DeprecationWarning` but still resolves to the correct symbol
- [ ] CHANGELOG contains the import-migration table for downstream consumers
- [ ] Existing consumer `zephlib` passes its own test suite against the new `hyperion-sdk` 1.0.0 (with appropriate extras pinned: `hyperion-sdk[catalog]` for the modules that use Catalog)

---

## Estimated shape (not a schedule)

- Steps 1–3: ~1 day each, low risk, mostly mechanical
- Step 4: ~1 day (StoragePort design + adapters)
- Step 5: ~2–3 days (the actual Catalog inversion, plus new filesystem-storage tests)
- Steps 6–9: ~1 day each
- Step 10: ~1 day (pyproject + CI)
- Step 11: ~half day

A patient pace lets the refactor sit between other work without forcing a sprint. Each step ships independently — if anything stalls, the package is still in a coherent state.
