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
    program = (
        f"import {module_name}\n"
        "import sys, json\n"
        "print(json.dumps(list(sys.modules.keys())))\n"
    )
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


# -- These modules currently violate the promise; the refactor fixes them --
# `strict=True` means the marker fails CI as soon as the test starts passing —
# forcing the marker to be removed when the corresponding refactor step lands.

@pytest.mark.xfail(
    strict=True,
    reason="Refactor F5 / Step 2: typeutils.py must split stdlib vs pandera/polars half.",
)
def test_typeutils_does_not_pull_data_stack() -> None:
    assert _heavy_pulled_in("hyperion.typeutils") == set()


@pytest.mark.xfail(
    strict=True,
    reason="Refactor F6 / Step 2: entities.catalog Asset identity must drop pandera/polars.",
)
def test_entities_catalog_does_not_pull_data_stack() -> None:
    assert _heavy_pulled_in("hyperion.entities.catalog") == set()


@pytest.mark.xfail(
    strict=True,
    reason="Refactor F8 / Step 9: hyperion.catalog namespace must not eagerly load fastavro/boto3.",
)
def test_catalog_namespace_lazy() -> None:
    assert _heavy_pulled_in("hyperion.catalog") == set()
