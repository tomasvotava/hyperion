"""Import-graph regression tests for the DDD refactor's lite-core promise.

These tests subprocess-import a module and check what's in `sys.modules`. Heavy
third-party deps must not be pulled in when consumers only import lite-core
utilities.

Modules that pass today stay green. Modules that currently violate the promise
are marked `xfail(strict=True)` with the refactor step that will fix them — once
the step lands, the marker flips to XPASS and CI fails until the marker is removed.
"""

import json
import subprocess
import sys

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
    hits: set[str] = set()
    for heavy in HEAVY_MODULES:
        if any(m == heavy or m.startswith(f"{heavy}.") for m in loaded):
            hits.add(heavy)
    return hits


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
