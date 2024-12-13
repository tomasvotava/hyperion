import asyncio
import time
from collections.abc import AsyncIterable, AsyncIterator, Iterator
from typing import Any

import pytest

from hyperion.asyncutils import AsyncTaskQueue, aiter_any


async def async_iterator() -> AsyncIterator[int]:
    for i in range(5):
        yield i


def sync_iterator() -> Iterator[int]:
    yield from range(5)


class AsyncList(AsyncIterable[int]):
    def __init__(self, items: list[int]) -> None:
        self.items = items
        self.index = 0

    def __aiter__(self) -> "AsyncList":
        self.index = 0
        return self

    async def __anext__(self) -> int:
        if self.index < len(self.items):
            value = self.items[self.index]
            self.index += 1
            return value
        raise StopAsyncIteration


async def collect_test(subject: Any) -> set[int]:
    collected: set[int] = set()
    async for i in aiter_any(subject):
        collected.add(i)
    return collected


async def test_aiter_asyncgen() -> None:
    assert await collect_test(async_iterator()) == {0, 1, 2, 3, 4}


async def test_aiter_asyncgenfunction() -> None:
    assert await collect_test(async_iterator) == {0, 1, 2, 3, 4}


async def test_aiter_syncgen() -> None:
    assert await collect_test(sync_iterator()) == {0, 1, 2, 3, 4}


async def test_aiter_syncgenfunction() -> None:
    assert await collect_test(sync_iterator) == {0, 1, 2, 3, 4}


async def test_aiter_iterable() -> None:
    assert await collect_test((0, 1, 2, 3, 4)) == {0, 1, 2, 3, 4}


async def test_aiter_asynciterable() -> None:
    assert await collect_test(AsyncList([0, 1, 2, 3, 4])) == {0, 1, 2, 3, 4}


async def test_aiter_unsupported() -> None:
    with pytest.raises(TypeError, match="Provided value cannot be iterated over."):
        await collect_test(None)


async def test_async_task_queue() -> None:
    class _Cororun:
        def __init__(self) -> None:
            self.called = 0

        async def task(self) -> None:
            await asyncio.sleep(1)
            self.called += 1

    cororun = _Cororun()
    start_time = time.monotonic()
    async with AsyncTaskQueue[None]() as queue:
        async for _ in aiter_any(range(5)):
            queue.add_task(cororun.task())
    duration = time.monotonic() - start_time
    assert cororun.called == 5
    assert duration >= 1
    assert duration < 2
