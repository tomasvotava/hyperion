"""Port: message queue abstraction.

Abstract :class:`Queue` base. Message models live in
:mod:`hyperion.domain.messages`; concrete adapters (``InMemoryQueue``,
``SQSQueue``, ``FileQueue``) live in ``hyperion.adapters.queue.*``. The
``from_config`` family reaches the concrete adapters via a deferred import.
"""

from __future__ import annotations

import abc
import os
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from hyperion.config import queue_config
from hyperion.log import get_logger

if TYPE_CHECKING:
    from hyperion.domain.messages import Message

logger = get_logger("hyperion-queue")


class Queue(abc.ABC):
    _cached_instance: ClassVar[Queue | None] = None

    @staticmethod
    def _resolve_type_from_config() -> type[Queue]:
        from hyperion.adapters.queue.filesystem import FileQueue
        from hyperion.adapters.queue.memory import InMemoryQueue
        from hyperion.adapters.queue.sqs import SQSQueue

        if queue_config.url and queue_config.path:
            logger.warning(
                "Ambiguous configuration detected - both queue URL and queue path were set. Will default to SQS queue.",
                queue_url=queue_config.url,
                queue_path=queue_config.path,
            )
        if queue_config.url is not None:
            return SQSQueue
        if queue_config.path is not None:
            return FileQueue
        return InMemoryQueue

    @classmethod
    def _create_from_config(cls) -> Queue:
        from hyperion.adapters.queue.filesystem import FileQueue
        from hyperion.adapters.queue.memory import InMemoryQueue
        from hyperion.adapters.queue.sqs import SQSQueue

        queue_type = cls._resolve_type_from_config()
        logger.info(
            "Resolved queue type from configuration.",
            queue_type=queue_type,
            env={k: v for k, v in os.environ.items() if k.lower().startswith("hyperion")},
        )
        if queue_type is SQSQueue and queue_config.url is not None:
            logger.info("Using SQS queue.")
            return SQSQueue(queue_config.url)
        if queue_type is FileQueue and queue_config.path is not None:
            logger.info("Using FileQueue.")
            return FileQueue(Path(queue_config.path), overwrite=queue_config.path_overwrite)
        if queue_type is InMemoryQueue:
            logger.info("Using in-memory queue.")
            return InMemoryQueue()
        raise ValueError(f"Unknown queue type {queue_type!r} or missing configuration options.")

    @classmethod
    def from_config(cls) -> Queue:
        if cls._cached_instance is None:
            cls._cached_instance = cls._create_from_config()
        if cls._cached_instance.__class__ is not (resolved_type := cls._resolve_type_from_config()):
            raise RuntimeError(
                f"Configuration inconsistency - cached queue instance is of type {type(cls._cached_instance)!r}, "
                f"but type requested by configuration is {resolved_type!r}."
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
