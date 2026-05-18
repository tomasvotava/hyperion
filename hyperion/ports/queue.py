"""Port: message queue abstraction.

Abstract :class:`Queue` base. Message models live in
:mod:`hyperion.domain.messages`; concrete adapters (``InMemoryQueue``,
``SQSQueue``, ``FileQueue``) live in ``hyperion.adapters.queue.*``. The
``from_config`` family delegates backend selection to
:mod:`hyperion.composition` (the single composition root).
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from hyperion.domain.messages import Message


class Queue(abc.ABC):
    _cached_instance: ClassVar[Queue | None] = None

    @classmethod
    def _create_from_config(cls) -> Queue:
        from hyperion import composition

        return composition.default_queue()

    @classmethod
    def from_config(cls) -> Queue:
        from hyperion import composition

        if cls._cached_instance is None:
            cls._cached_instance = cls._create_from_config()
        expected_class_name = composition.QUEUE_BACKEND_CLASS_NAMES[composition.resolve_queue_backend()]
        if cls._cached_instance.__class__.__name__ != expected_class_name:
            raise RuntimeError(
                f"Configuration inconsistency - cached queue instance is of type {type(cls._cached_instance)!r}, "
                f"but type requested by configuration is {expected_class_name!r}."
            )
        return cls._cached_instance

    @abc.abstractmethod
    def send(self, message: Message) -> None:
        """Send a message to the queue."""

    @abc.abstractmethod
    def delete(self, receipt_handle: str) -> None:
        """Delete a message from the queue."""

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
