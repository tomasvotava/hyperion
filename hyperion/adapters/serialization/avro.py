"""Avro serialization adapter (requires the ``[catalog]`` extra -- fastavro).

This is the *only* module that imports :mod:`fastavro`. It was extracted out of
:class:`hyperion.catalog.catalog.Catalog` (DDD refactor F1 / Step 5) so that the
catalog depends on an injected serializer instead of reaching for fastavro at
module scope. ``Catalog`` default-constructs an :class:`AvroSerializer` when no
serializer is supplied; importing this module is what gates the fastavro
requirement, so the error below points at the extra to install.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import IO, Any, Protocol, runtime_checkable

try:
    import fastavro
    import fastavro.write
except ImportError as error:  # pragma: no cover - exercised only without [catalog]
    raise ImportError(
        "Avro serialization requires the optional 'catalog' extra. "
        "Install it with: pip install 'hyperion-sdk[catalog]'."
    ) from error


@runtime_checkable
class AvroStreamWriter(Protocol):
    """Incremental avro writer (one record at a time, then flushed).

    Structural type for the object :meth:`AvroSerializer.streaming_writer`
    returns, so callers (e.g. ``AssetRepartitioner``) need no fastavro import.
    """

    def write(self, record: dict[str, Any]) -> None:
        """Append a single record to the open avro stream."""
        ...

    def dump(self) -> None:
        """Flush all buffered records and the avro footer to the file."""
        ...


class AvroSerializer:
    """Encapsulates the catalog's fastavro encode/decode contract.

    The encode parameters (``deflate`` codec, compression level 7, validation,
    lenient strictness) are pinned here so byte-level output is identical to the
    pre-refactor inline implementation.
    """

    def write(
        self,
        fp: IO[bytes],
        schema: dict[str, Any],
        records: Iterable[dict[str, Any]],
        metadata: dict[str, str],
    ) -> None:
        """Write ``records`` to ``fp`` as a single avro container file."""
        fastavro.writer(
            fp,
            records=records,
            schema=schema,
            codec="deflate",
            validator=True,
            codec_compression_level=7,
            strict=False,
            strict_allow_default=True,
            metadata=metadata,
        )

    def read(self, fp: IO[bytes]) -> Iterator[Any]:
        """Yield raw records decoded from the avro container in ``fp``.

        Type/shape validation of each row stays with the caller.
        """
        yield from fastavro.reader(fp)

    def streaming_writer(self, fp: IO[bytes], schema: dict[str, Any], metadata: dict[str, str]) -> AvroStreamWriter:
        """Open an incremental avro writer over ``fp`` (for repartitioning).

        Mirrors the pre-refactor ``fastavro.write.Writer`` configuration (no
        explicit compression level / strictness flags).
        """
        return fastavro.write.Writer(
            fp,
            schema=schema,
            codec="deflate",
            validator=True,
            metadata=metadata,
        )
