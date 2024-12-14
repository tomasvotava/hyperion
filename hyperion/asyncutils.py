import asyncio
import inspect
import sys
from collections.abc import AsyncIterable, AsyncIterator, Callable, Coroutine, Iterable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from hyperion.logging import get_logger

if sys.version_info >= (3, 11) and TYPE_CHECKING:
    from typing import Self

if sys.version_info < (3, 11) and TYPE_CHECKING:
    from typing_extensions import Self

T = TypeVar("T")


logger = get_logger("hyperion-asyncutils")


async def iter_async(iterable: Iterable[T]) -> AsyncIterator[T]:
    for item in iterable:
        yield item


async def aiter_any(
    iterable: Iterable[T] | AsyncIterable[T] | Callable[[], Iterable[T] | AsyncIterable[T]],
) -> AsyncIterator[T]:
    asyncgen: AsyncIterable[T]
    if inspect.isasyncgen(iterable) or isinstance(iterable, AsyncIterable):
        asyncgen = iterable
    elif inspect.isasyncgenfunction(iterable):
        asyncgen = iterable()
    elif inspect.isgenerator(iterable) or isinstance(iterable, Iterable):
        asyncgen = iter_async(iterable)
    elif inspect.isgeneratorfunction(iterable):
        asyncgen = iter_async(iterable())
    else:
        raise TypeError("Provided value cannot be iterated over.")
    async for item in asyncgen:
        yield item


class AsyncTaskQueue(Generic[T]):
    def __init__(self, maxsize: int = 0) -> None:
        self.tasklist: list[asyncio.Task[T]] = []
        self._entered = False
        self.maxsize = maxsize

    async def _flush(self) -> None:
        logger.debug(f"Max size of {self.maxsize} reached, waiting for enqueued tasks to finish.")
        await asyncio.gather(*self.tasklist)
        self.tasklist = []

    async def __aenter__(self) -> "Self":
        self.tasklist = []
        self._entered = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._entered = False
        await self._flush()

    async def add_task(self, coroutine: Coroutine[Any, Any, T]) -> None:
        if not self._entered:
            raise RuntimeError(f"{self.__class__.__name__!r} must be used in an 'async with' context.")
        if self.maxsize and len(self.tasklist) >= self.maxsize:
            await self._flush()
        self.tasklist.append(asyncio.create_task(coroutine))
