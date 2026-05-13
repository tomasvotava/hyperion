"""Smoke tests for `hyperion.sources.base.Source`.

The Source ABC is moved-but-unchanged by F9 — its import path may shift between
`hyperion.sources.base` and a (future) explicit location, and its constructor
still takes a `Catalog`. These tests assert the minimal subclass contract so
import churn during the refactor surfaces as a test failure rather than a
runtime crash in zephlib.
"""

from collections.abc import AsyncIterator, Awaitable, Iterable

import pytest

from hyperion.sources.base import Source, SourceAsset, SourceParamsType


class DummySource(Source):
    source = "dummy"

    def run(
        self,
        start_date: object = None,
        end_date: object = None,
        params: SourceParamsType | None = None,
    ) -> Awaitable[Iterable[SourceAsset]] | AsyncIterator[SourceAsset]:
        raise NotImplementedError


class TestSource:
    def test_subclass_with_source_name_works(self) -> None:
        # Pass a sentinel for the catalog so we don't construct one.
        sentinel: object = object()
        instance = DummySource(sentinel)  # type: ignore[arg-type]
        assert instance.catalog is sentinel
        assert instance.source == "dummy"

    def test_subclass_without_source_name_raises(self) -> None:
        class MissingNameSource(Source):
            # Inherits source = NotImplemented from the base.
            def run(
                self,
                start_date: object = None,
                end_date: object = None,
                params: SourceParamsType | None = None,
            ) -> Awaitable[Iterable[SourceAsset]] | AsyncIterator[SourceAsset]:
                raise NotImplementedError

        with pytest.raises(NotImplementedError, match="source name"):
            MissingNameSource(object())  # type: ignore[arg-type]
