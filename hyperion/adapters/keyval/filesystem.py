"""Filesystem-backed :class:`KeyValueStore` adapter (lite -- stdlib only).

One file per (hashed, prefixed) key under a root directory, mirroring the
``LocalFileCache`` idiom. Keys are url-safe-base64 encoded into filenames so
arbitrary keys (containing ``/``, ``:`` ...) are safe and reversible. Writes
are atomic (write to a temp file in the same directory, then ``os.replace``).
"""

from __future__ import annotations

import base64
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

from hyperion.log import get_logger
from hyperion.ports.keyval import CompressionType, KeyValueStore

logger = get_logger("adapters.keyval.filesystem")


class FilesystemStore(KeyValueStore):
    """A persistent key-value store backed by a directory of files."""

    def __init__(
        self,
        root_path: Path | str,
        prefix: str | None = None,
        compression: CompressionType | None = None,
    ) -> None:
        super().__init__(prefix, compression)
        self.root_path = Path(root_path)
        if self.root_path.exists() and not self.root_path.is_dir():
            raise ValueError(f"Given key-value store path ({self.root_path.as_posix()}) is not a directory.")
        self.root_path.mkdir(parents=True, exist_ok=True)
        logger.info("Initialized FilesystemStore.", root_path=self.root_path.as_posix())

    @staticmethod
    def _encode(hashed_key: str) -> str:
        return base64.urlsafe_b64encode(hashed_key.encode("utf-8")).decode("ascii")

    @staticmethod
    def _decode(filename: str) -> str:
        return base64.urlsafe_b64decode(filename.encode("ascii")).decode("utf-8")

    def _path(self, hashed_key: str) -> Path:
        return self.root_path / self._encode(hashed_key)

    def _get_raw(self, hashed_key: str) -> bytes | None:
        key_path = self._path(hashed_key)
        if not key_path.exists():
            return None
        return key_path.read_bytes()

    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        key_path = self._path(hashed_key)
        with tempfile.NamedTemporaryFile("wb", dir=self.root_path, delete=False) as tmp_file:
            tmp_file.write(compresed_value)
            tmp_path = tmp_file.name
        os.replace(tmp_path, key_path)  # noqa: PTH105 - atomic same-dir replace

    def _delete_raw(self, hashed_key: str) -> None:
        self._path(hashed_key).unlink(missing_ok=True)

    def _iter_all_keys(self) -> Iterable[str]:
        with os.scandir(self.root_path) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue
                key = self._decode(entry.name)
                if self.prefix:
                    key = key.replace(f"{self.prefix}:", "", 1)
                yield key
