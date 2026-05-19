"""In-memory :class:`Queue` adapter (lite -- not persistent)."""

from __future__ import annotations

from uuid import uuid4

from hyperion.domain.messages import Message
from hyperion.ports.queue import Queue


class InMemoryQueue(Queue):
    def __init__(self) -> None:
        super().__init__()
        self._messages: list[Message] = []

    def send(self, message: Message) -> None:
        if message.receipt_handle is None:
            message.receipt_handle = uuid4().hex
        self._messages.append(message)

    def delete(self, receipt_handle: str) -> None:
        """Delete the message identified by *receipt_handle*.

        Matches on the ``receipt_handle`` field assigned by :meth:`send`.
        Deleting an unknown handle is a no-op.
        """
        for message in self._messages:
            if message.receipt_handle == receipt_handle:
                self._messages.remove(message)
                break
