"""Import-graph regression tests for the DDD refactor's lite-core promise.

These tests subprocess-import a module and check what's in `sys.modules`. Heavy
third-party deps must not be pulled in when consumers only import lite-core
utilities.

Modules that pass today stay green. Modules that currently violate the promise
are marked `xfail(strict=True)` with the refactor step that will fix them — once
the step lands, the marker flips to XPASS and CI fails until the marker is removed.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

HEAVY_MODULES = {
    "boto3",
    "botocore",
    "aioboto3",
    "polars",
    "pandera",
    "fastavro",
    "googlemaps",
    "snappy",
    "numpy",
}


def _loaded_modules(module_name: str) -> set[str]:
    """Spawn a fresh interpreter, import the module, return `sys.modules.keys()`."""
    program = f"import {module_name}\nimport sys, json\nprint(json.dumps(list(sys.modules.keys())))\n"
    result = subprocess.run(  # noqa: S603 - trusted args (sys.executable + literal program)
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"Importing {module_name!r} failed: {result.stderr}")
    last_line = [line for line in result.stdout.strip().splitlines() if line][-1]
    return set(json.loads(last_line))


def _heavy_pulled_in(module_name: str) -> set[str]:
    loaded = _loaded_modules(module_name)
    return _heavy_in(loaded)


def _heavy_in(loaded: set[str]) -> set[str]:
    hits: set[str] = set()
    for heavy in HEAVY_MODULES:
        if any(m == heavy or m.startswith(f"{heavy}.") for m in loaded):
            hits.add(heavy)
    return hits


# Required-no-default storage fields, so `import hyperion.composition` /
# `hyperion.config` construct cleanly in a hermetic subprocess even though the
# lite factories below never read these values.
_LITE_ENV = {
    "HYPERION_STORAGE_DATA_LAKE_BUCKET": "dl",
    "HYPERION_STORAGE_FEATURE_STORE_BUCKET": "fs",
    "HYPERION_STORAGE_PERSISTENT_STORE_BUCKET": "ps",
    "HYPERION_STORAGE_SCHEMA_PATH": "file:///tmp/hyperion-s8-schemas",
}


def _modules_after_factory(setup: str, call: str, env: dict[str, str]) -> set[str]:
    """Fresh interpreter with a hermetic env: run `setup`, run `call`, dump `sys.modules`.

    All inherited ``HYPERION_*`` vars are stripped so backend selection is
    deterministic regardless of the dev/CI environment; ``_LITE_ENV`` + the
    per-test ``env`` overrides are the only hyperion config the subprocess sees.
    """
    program = f"{setup}\n{call}\nimport sys, json\nprint(json.dumps(list(sys.modules.keys())))\n"
    subprocess_env = {k: v for k, v in os.environ.items() if not k.startswith("HYPERION_")}
    subprocess_env.update(_LITE_ENV)
    subprocess_env.update(env)
    # Run from an isolated cwd so config.py's load_dotenv(find_dotenv(usecwd=True))
    # can't re-introduce a repo/ancestor .env that would defeat the env strip.
    with tempfile.TemporaryDirectory() as isolated_cwd:
        result = subprocess.run(  # noqa: S603 - trusted args (sys.executable + literal program)
            [sys.executable, "-c", program],
            capture_output=True,
            text=True,
            check=False,
            env=subprocess_env,
            cwd=isolated_cwd,
        )
    if result.returncode != 0:
        pytest.fail(f"factory subprocess failed ({setup!r} / {call!r}): {result.stderr}")
    last_line = [line for line in result.stdout.strip().splitlines() if line][-1]
    return set(json.loads(last_line))


# -- These modules must remain lite today and forever --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.log",
        "hyperion.dateutils",
        "hyperion.asyncutils",
        "hyperion.config",
    ],
)
def test_lite_core_module_pulls_no_heavy_deps(module: str) -> None:
    hits = _heavy_pulled_in(module)
    assert hits == set(), f"{module} unexpectedly imports {sorted(hits)}"


# -- Ports layer (S1): every port module must import in a fresh interpreter.
#    Two of them (queue, schema_registry) reference Message / AssetProtocol /
#    AssetType only in annotations, so they must NOT pull boto3 or the data
#    stack -- this guards the deferred-import / TYPE_CHECKING wiring. The cache
#    and keyval ports legitimately use snappy for (de)compression, so snappy is
#    deliberately not asserted against. --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.ports",
        "hyperion.ports.cache",
        "hyperion.ports.queue",
        "hyperion.ports.keyval",
        "hyperion.ports.secrets",
        "hyperion.ports.schema_registry",
        "hyperion.ports.storage",
        "hyperion.ports.geocoder",
    ],
)
def test_ports_modules_import(module: str) -> None:
    # _loaded_modules fails the test if the import errors out.
    assert module in _loaded_modules(module)


@pytest.mark.parametrize("module", ["hyperion.ports.queue", "hyperion.ports.schema_registry"])
def test_annotation_only_ports_stay_lite(module: str) -> None:
    hits = _heavy_pulled_in(module)
    forbidden = {"boto3", "botocore", "polars", "pandera", "fastavro", "numpy"} & hits
    assert forbidden == set(), f"{module} unexpectedly imports {sorted(forbidden)}"


# -- S2 (F5/F6) landed: typeutils.py split stdlib vs the pandera/polars half,
#    and the entities.catalog / domain.assets identity layer no longer pulls
#    the data stack. These must stay lite today and forever. --


def test_typeutils_does_not_pull_data_stack() -> None:
    assert _heavy_pulled_in("hyperion.typeutils") == set()


def test_entities_catalog_does_not_pull_data_stack() -> None:
    assert _heavy_pulled_in("hyperion.entities.catalog") == set()


def test_domain_assets_does_not_pull_data_stack() -> None:
    assert _heavy_pulled_in("hyperion.domain.assets") == set()


# -- S3 (F2) landed: hyperion.domain.geo is pure data + haversine math. It must
#    not pull boto3 / the data stack, and -- crucially for F2 -- must not pull
#    the cache port (the Location._cache singleton that reached boto3 via
#    Cache.from_config is gone). `numpy` is deliberately NOT asserted against:
#    `haversine` (lite core) does an optional `import numpy` at module load
#    *iff numpy is installed* (its vector fast-path); it is not a declared
#    haversine dependency and the scalar path Location uses works without it, so
#    SpatialKMeans imports numpy lazily and numpy is [data]-gated at the install
#    level (Step 10 / the no-extras smoke test), not by this in-env guard. --


def test_domain_geo_stays_lite() -> None:
    forbidden = {"boto3", "botocore", "aioboto3", "polars", "pandera", "fastavro", "googlemaps"} & _heavy_pulled_in(
        "hyperion.domain.geo"
    )
    assert forbidden == set(), f"hyperion.domain.geo unexpectedly imports {sorted(forbidden)}"


def test_domain_geo_does_not_pull_cache_port() -> None:
    assert "hyperion.ports.cache" not in _loaded_modules("hyperion.domain.geo")


# -- S4 (F1 part 1) landed: StoragePort + adapters. The memory/filesystem
#    adapters and the adapters namespace are lite -- importing them must not
#    drag boto3 / the data stack. (hyperion.adapters.storage.s3 legitimately
#    imports boto3 and is deliberately excluded here.) --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.adapters",
        "hyperion.adapters.storage",
        "hyperion.adapters.storage.memory",
        "hyperion.adapters.storage.filesystem",
    ],
)
def test_lite_storage_adapters_pull_no_heavy_deps(module: str) -> None:
    hits = _heavy_pulled_in(module)
    assert hits == set(), f"{module} unexpectedly imports {sorted(hits)}"


# -- This module currently violates the promise; the refactor fixes it --
# `strict=True` means the marker fails CI as soon as the test starts passing —
# forcing the marker to be removed when the corresponding refactor step lands.


@pytest.mark.xfail(
    strict=True,
    reason="Refactor F8 / Step 9: hyperion.catalog namespace must not eagerly load fastavro/boto3.",
)
def test_catalog_namespace_lazy() -> None:
    assert _heavy_pulled_in("hyperion.catalog") == set()


# -- S6 (F7) landed: concrete adapters relocated to hyperion.adapters.<port>.*
#    and the message models to hyperion.domain.messages. The lite adapters and
#    the domain message module must not drag boto3 / the data stack. (The
#    ``dynamodb`` / ``sqs`` / ``aws_sm`` / ``s3`` adapters legitimately import
#    boto3 and are deliberately excluded here.) --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.domain.messages",
        "hyperion.adapters.cache",
        "hyperion.adapters.cache.memory",
        "hyperion.adapters.cache.filesystem",
        "hyperion.adapters.keyval.memory",
        "hyperion.adapters.keyval.filesystem",
        "hyperion.adapters.queue.memory",
        "hyperion.adapters.queue.filesystem",
        "hyperion.adapters.secrets.dummy",
        "hyperion.adapters.secrets.env",
        "hyperion.adapters.schema_registry.local",
        "hyperion.adapters.http.proxy",
    ],
)
def test_s6_lite_adapters_pull_no_heavy_deps(module: str) -> None:
    # ``snappy`` is deliberately not asserted against: the cache / keyval ports
    # ``import snappy`` for (de)compression, so any adapter importing those ports
    # transitively loads it. snappy is ``[snappy]``-gated at the install level
    # (Step 10), not by this in-env guard -- same carve-out as numpy / haversine.
    forbidden = (HEAVY_MODULES - {"snappy"}) & _heavy_pulled_in(module)
    assert forbidden == set(), f"{module} unexpectedly imports {sorted(forbidden)}"


# -- S6 deferred-shim guarantee: importing a deprecated old path must not pull
#    boto3 (the moved concretes are resolved lazily via __getattr__). The pure
#    infra shims must also not drag the data stack. --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.infrastructure.keyval",
        "hyperion.infrastructure.secretsmanager",
        "hyperion.infrastructure.message_queue",
        "hyperion.infrastructure.httputils",
        # S7 cut the PersistentCache -> Catalog knot: importing the cache shim no
        # longer drags Catalog / fastavro / boto3 (PersistentCache resolves lazily
        # to hyperion.application.persistent_cache via __getattr__).
        "hyperion.infrastructure.cache",
    ],
)
def test_pure_shims_pull_no_heavy_deps(module: str) -> None:
    # snappy rides along via the cache / keyval ports (see note above); the
    # contract that matters for the deferred shims is "no boto3 / data stack".
    forbidden = (HEAVY_MODULES - {"snappy"}) & _heavy_pulled_in(module)
    assert forbidden == set(), f"{module} unexpectedly imports {sorted(forbidden)}"


# ``hyperion.catalog.schema`` touches the eager ``hyperion.catalog`` namespace
# (-> fastavro), status quo until S9 (F8: test_catalog_namespace_lazy is xfail).
# The S6 contract here is narrower: the deferred __getattr__ must never pull
# boto3 for the relocated concretes. (``hyperion.infrastructure.cache`` graduated
# to the stricter pure-shim guard above once S7 cut the Catalog knot.)


@pytest.mark.parametrize("module", ["hyperion.catalog.schema"])
def test_deferred_shims_do_not_pull_boto3(module: str) -> None:
    assert "boto3" not in _heavy_pulled_in(module)


# -- S7 (F3 / F4) landed: GoogleMaps moved to hyperion.adapters.geocoder.google
#    (implements the Geocoder port, takes an injected KeyValueStore -- no more
#    PersistentCache / Catalog knot). The geocoder port + the static adapter are
#    lite; the google adapter legitimately imports googlemaps (excluded, like
#    s3/dynamodb) but must not drag boto3 / the data stack. The deprecated old
#    geo paths must resolve googlemaps lazily (no pull on shim import). numpy is
#    carved out: hyperion.domain.geo -> haversine soft-imports it (same carve-out
#    as test_domain_geo_stays_lite). --


@pytest.mark.parametrize(
    "module",
    [
        "hyperion.adapters.geocoder",
        "hyperion.adapters.geocoder.static",
        "hyperion.ports.geocoder",
    ],
)
def test_s7_lite_geocoder_pulls_no_heavy_deps(module: str) -> None:
    forbidden = (HEAVY_MODULES - {"numpy"}) & _heavy_pulled_in(module)
    assert forbidden == set(), f"{module} unexpectedly imports {sorted(forbidden)}"


def test_s7_google_adapter_stays_off_boto3_and_data_stack() -> None:
    # googlemaps + snappy (via the keyval port) + numpy (via haversine) ride
    # along legitimately; boto3 / the data stack must not.
    forbidden = {"boto3", "botocore", "aioboto3", "polars", "pandera", "fastavro"} & _heavy_pulled_in(
        "hyperion.adapters.geocoder.google"
    )
    assert forbidden == set(), f"adapters.geocoder.google unexpectedly imports {sorted(forbidden)}"


@pytest.mark.parametrize("module", ["hyperion.infrastructure.geo.gmaps", "hyperion.infrastructure.geo"])
def test_s7_deprecated_geo_paths_defer_googlemaps(module: str) -> None:
    # Importing the deprecated shim must not pull googlemaps (resolved lazily via
    # __getattr__) nor boto3 / the data stack. numpy carve-out: the geo package
    # __init__ re-exports Location from hyperion.domain.geo -> haversine.
    forbidden = (HEAVY_MODULES - {"numpy"}) & _heavy_pulled_in(module)
    assert forbidden == set(), f"{module} unexpectedly imports {sorted(forbidden)}"


# -- S8 (F9) landed: the composition root centralizes from_config wiring and
#    imports the selected adapter *inside* its config branch. The central
#    promise of epic #137: calling `.from_config()` on a lite install whose
#    config selects a memory/filesystem backend must NOT import boto3 (the
#    AWS-only adapter modules `import boto3` at module level, so the old
#    "import all backends, then branch" factories crashed lite installs). One
#    assertion per delegating factory. snappy/numpy ride along legitimately via
#    the cache/keyval ports + haversine (same carve-out as the S6/S7 guards);
#    the contract that matters is "no boto3 / botocore / aioboto3 / fastavro". --

_S8_AWS_AND_CATALOG_DEPS = {"boto3", "botocore", "aioboto3", "fastavro"}


def test_composition_module_stays_lite() -> None:
    # Importing the composition root itself must pull nothing heavy -- every
    # adapter import is deferred inside a config branch.
    assert _heavy_pulled_in("hyperion.composition") == set()


def test_cache_from_config_lite_pulls_no_boto3() -> None:
    # No HYPERION_STORAGE_CACHE_* -> InMemoryCache.
    loaded = _modules_after_factory("from hyperion.ports.cache import Cache", "Cache.from_config()", env={})
    forbidden = _S8_AWS_AND_CATALOG_DEPS & _heavy_in(loaded)
    assert forbidden == set(), f"Cache.from_config() (lite) pulled {sorted(forbidden)}"


def test_queue_from_config_lite_pulls_no_boto3() -> None:
    # No HYPERION_QUEUE_* -> InMemoryQueue; the config-consistency check must
    # also not import SQSQueue (it resolves a backend *name*, not a type).
    loaded = _modules_after_factory("from hyperion.ports.queue import Queue", "Queue.from_config()", env={})
    forbidden = _S8_AWS_AND_CATALOG_DEPS & _heavy_in(loaded)
    assert forbidden == set(), f"Queue.from_config() (lite) pulled {sorted(forbidden)}"


def test_filequeue_from_config_lite_pulls_no_boto3(tmp_path: Path) -> None:
    # A non-memory lite backend (FileQueue) still must not pull boto3 -- proves
    # the consistency check is boto3-free even when a backend is configured.
    loaded = _modules_after_factory(
        "from hyperion.ports.queue import Queue",
        "Queue.from_config()",
        env={"HYPERION_QUEUE_PATH": f"{tmp_path}/queue.jsonl"},
    )
    forbidden = _S8_AWS_AND_CATALOG_DEPS & _heavy_in(loaded)
    assert forbidden == set(), f"Queue.from_config() (FileQueue) pulled {sorted(forbidden)}"


def test_secrets_from_config_lite_pulls_no_boto3() -> None:
    # No HYPERION_SECRETS_BACKEND -> DummySecretsManager.
    loaded = _modules_after_factory(
        "from hyperion.ports.secrets import SecretsManager", "SecretsManager.from_config()", env={}
    )
    forbidden = _S8_AWS_AND_CATALOG_DEPS & _heavy_in(loaded)
    assert forbidden == set(), f"SecretsManager.from_config() (lite) pulled {sorted(forbidden)}"


def test_schema_registry_from_config_lite_pulls_no_boto3(tmp_path: Path) -> None:
    # A file:// schema path -> LocalSchemaStore (the S3SchemaStore branch, which
    # transitively pulls boto3, must not be imported). LocalSchemaStore validates
    # the path exists (unchanged behaviour), so create it first.
    schemas_dir = tmp_path / "schemas"
    schemas_dir.mkdir()
    loaded = _modules_after_factory(
        "from hyperion.ports.schema_registry import SchemaStore",
        "SchemaStore.from_config()",
        env={"HYPERION_STORAGE_SCHEMA_PATH": schemas_dir.as_uri()},
    )
    forbidden = _S8_AWS_AND_CATALOG_DEPS & _heavy_in(loaded)
    assert forbidden == set(), f"SchemaStore.from_config() (lite) pulled {sorted(forbidden)}"
