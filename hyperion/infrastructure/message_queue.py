"""Deprecated import shim for message models and queue adapters.

.. deprecated::
    The abstract :class:`Queue` moved to :mod:`hyperion.ports.queue`. The
    message models and their SQS-envelope helpers moved to
    :mod:`hyperion.domain.messages`. The concrete adapters moved to
    ``hyperion.adapters.queue.*`` (``InMemoryQueue`` ->
    :mod:`hyperion.adapters.queue.memory`, ``FileQueue`` ->
    :mod:`hyperion.adapters.queue.filesystem`, ``SQSQueue`` ->
    :mod:`hyperion.adapters.queue.sqs`). Import them from there. This module
    keeps every symbol importable (with a :class:`DeprecationWarning`,
    resolved lazily so the import does not pull boto3) for the whole
    ``hyperion-sdk`` 1.x line. The symbols are removed in 2.0.
"""

import importlib
from typing import TYPE_CHECKING

from hyperion._compat import moved_attr
from hyperion.ports.queue import Queue as _Queue

if TYPE_CHECKING:
    from hyperion.adapters.queue.filesystem import FileQueue
    from hyperion.adapters.queue.memory import InMemoryQueue
    from hyperion.adapters.queue.sqs import SQSQueue
    from hyperion.domain.messages import (
        ArrivalEvent,
        DataLakeArrivalMessage,
        Message,
        SerializedMessage,
        SourceBackfillMessage,
        create_backfill_event,
        iter_messages_from_sqs_event,
    )
    from hyperion.ports.queue import Queue

_OLD_MODULE = "hyperion.infrastructure.message_queue"

_MOVED: dict[str, tuple[object, str]] = {
    "Queue": (_Queue, "hyperion.ports.queue"),
}

_MOVED_LAZY: dict[str, str] = {
    "ArrivalEvent": "hyperion.domain.messages",
    "Message": "hyperion.domain.messages",
    "DataLakeArrivalMessage": "hyperion.domain.messages",
    "SourceBackfillMessage": "hyperion.domain.messages",
    "SerializedMessage": "hyperion.domain.messages",
    "iter_messages_from_sqs_event": "hyperion.domain.messages",
    "create_backfill_event": "hyperion.domain.messages",
    "InMemoryQueue": "hyperion.adapters.queue.memory",
    "FileQueue": "hyperion.adapters.queue.filesystem",
    "SQSQueue": "hyperion.adapters.queue.sqs",
}

__all__ = [
    "ArrivalEvent",
    "DataLakeArrivalMessage",
    "FileQueue",
    "InMemoryQueue",
    "Message",
    "Queue",
    "SQSQueue",
    "SerializedMessage",
    "SourceBackfillMessage",
    "create_backfill_event",
    "iter_messages_from_sqs_event",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        value, new_module = _MOVED[name]
        return moved_attr(name=name, value=value, old_module=_OLD_MODULE, new_module=new_module)
    if name in _MOVED_LAZY:
        new_module = _MOVED_LAZY[name]
        module = importlib.import_module(new_module)
        return moved_attr(name=name, value=getattr(module, name), old_module=_OLD_MODULE, new_module=new_module)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
