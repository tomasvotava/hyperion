"""In-memory :class:`Queue` adapter (lite -- not persistent)."""

from __future__ import annotations

from hyperion.domain.messages import Message
from hyperion.ports.queue import Queue


class InMemoryQueue(Queue):
    def __init__(self) -> None:
        super().__init__()
        self._messages: list[Message] = []

    def send(self, message: Message) -> None:
        self._messages.append(message)

    def delete(self, receipt_handle: str) -> None:
        """Delete the message using the created as isoformat."""
        for message in self._messages:
            if message.created.isoformat() == receipt_handle:
                self._messages.remove(message)
                break
