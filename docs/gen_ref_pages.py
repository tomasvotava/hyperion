"""mkdocs-gen-files generator: one API-reference page per public v1.0 module.

This script is run by the ``gen-files`` mkdocs plugin at build time. It writes
``reference/<path>.md`` (each just a ``:::`` mkdocstrings autodoc directive) plus
``reference/SUMMARY.md`` which ``literate-nav`` consumes to build the Reference
navigation. Nothing it writes is committed (see ``.gitignore``).

The module list is an *explicit allow-list* of the v1.0 public surface. The
deprecation shims -- ``hyperion.entities.*``, ``hyperion.infrastructure.*``,
the ``hyperion.catalog`` package shim, ``hyperion.application.persistent_cache``,
``hyperion.catalog.schema``, ``hyperion._compat`` -- are intentionally absent so
they never appear in the published reference. (griffe analyses statically and
never executes the PEP-562 ``__getattr__`` shims anyway; the allow-list is the
single source of truth for "what is v1.0 public".)

Lives outside ``hyperion/`` and ``tests/`` so it is not covered by the
pre-commit ``mypy hyperion tests`` / ``ruff check hyperion tests`` hooks.
"""

from pathlib import Path

import mkdocs_gen_files

PUBLIC_MODULES = [
    # Catalog orchestration
    "hyperion.catalog.catalog",
    # Domain layer (pure)
    "hyperion.domain.assets",
    "hyperion.domain.messages",
    "hyperion.domain.geo",
    # Ports (interfaces)
    "hyperion.ports.cache",
    "hyperion.ports.storage",
    "hyperion.ports.queue",
    "hyperion.ports.schema_registry",
    "hyperion.ports.secrets",
    "hyperion.ports.keyval",
    "hyperion.ports.geocoder",
    # Adapters (concrete backends)
    "hyperion.adapters.storage.s3",
    "hyperion.adapters.storage.filesystem",
    "hyperion.adapters.storage.memory",
    "hyperion.adapters.cache.dynamodb",
    "hyperion.adapters.cache.filesystem",
    "hyperion.adapters.cache.memory",
    "hyperion.adapters.keyval.dynamodb",
    "hyperion.adapters.keyval.filesystem",
    "hyperion.adapters.keyval.memory",
    "hyperion.adapters.queue.sqs",
    "hyperion.adapters.queue.filesystem",
    "hyperion.adapters.queue.memory",
    "hyperion.adapters.secrets.aws_sm",
    "hyperion.adapters.secrets.env",
    "hyperion.adapters.secrets.dummy",
    "hyperion.adapters.schema_registry.s3",
    "hyperion.adapters.schema_registry.local",
    "hyperion.adapters.geocoder.google",
    "hyperion.adapters.geocoder.static",
    "hyperion.adapters.serialization.avro",
    "hyperion.adapters.http.proxy",
    # Composition root
    "hyperion.composition",
    # Sources framework
    "hyperion.sources.base",
    "hyperion.sources.cli",
    # Repository / collections
    "hyperion.repository.asset_collection",
    # Data validation (requires the [data] extra)
    "hyperion.data.asset_schemas",
    # Lite-core utilities
    "hyperion.config",
    "hyperion.dateutils",
    "hyperion.asyncutils",
    "hyperion.typeutils",
    "hyperion.log",
]

nav = mkdocs_gen_files.Nav()

for dotted in PUBLIC_MODULES:
    parts = dotted.split(".")
    rel = Path(*parts[1:])  # drop the leading "hyperion"
    doc_path = Path("reference", rel).with_suffix(".md")

    nav[tuple(parts[1:])] = doc_path.relative_to("reference").as_posix()

    with mkdocs_gen_files.open(doc_path, "w") as fd:
        fd.write(f"# `{dotted}`\n\n::: {dotted}\n")

# The whole Reference section is literate-nav driven from this SUMMARY.md
# (mkdocs.yml has a single `- Reference: reference/` entry). Prepend the
# hand-written migration page, then nest the generated API tree under an
# "API reference" section.
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as fd:
    fd.write("* [Migrating from pre-1.0](migration-pre-1.0.md)\n")
    fd.write("* API reference\n")
    for line in nav.build_literate_nav():
        fd.write("    " + line)
