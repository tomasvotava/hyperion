"""`snappy` is the optional ``[snappy]`` extra (S10 / epic #137).

The cache and key-value ports import it gracefully; selecting ``snappy``
compression without the extra installed must fail at the (de)compression
call site with a clear, actionable ``ImportError`` -- never silently skip
compression (that would corrupt data across environments).

These tests simulate the extra being absent by monkeypatching the
module-level ``snappy`` to ``None`` (its value when the import failed).
"""

import pytest

import hyperion.ports.cache as cache_port
import hyperion.ports.keyval as keyval_port
from hyperion.adapters.cache.memory import InMemoryCache
from hyperion.adapters.keyval.memory import InMemoryStore

_HINT = r"hyperion-sdk\[snappy\]"


def test_cache_snappy_helpers_raise_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cache_port, "snappy", None)
    cache = InMemoryCache(prefix="t")
    with pytest.raises(ImportError, match=_HINT):
        cache._compress_bytes(b"payload")
    with pytest.raises(ImportError, match=_HINT):
        cache._decompress_bytes(b"payload")


def test_keyval_snappy_compression_raises_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(keyval_port, "snappy", None)
    store = InMemoryStore(compression="snappy")
    with pytest.raises(ImportError, match=_HINT):
        store._compress("payload")
    with pytest.raises(ImportError, match=_HINT):
        store._decompress(b"payload")


def test_keyval_non_snappy_compression_unaffected(monkeypatch: pytest.MonkeyPatch) -> None:
    """gzip / no-compression round-trips still work when snappy is absent."""
    monkeypatch.setattr(keyval_port, "snappy", None)
    for store in (InMemoryStore(compression="gzip"), InMemoryStore()):
        store.set("k", "value")
        assert store.get("k") == "value"
