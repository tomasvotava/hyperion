"""File-backed :class:`Queue` adapter (lite -- stdlib only)."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hyperion.domain.messages import Message, SerializedMessage
from hyperion.log import get_logger
from hyperion.ports.queue import Queue

if TYPE_CHECKING:
    from typing import Self

logger = get_logger("hyperion-queue")


class FileQueue(Queue):
    def __init__(self, queue_path: Path, *, overwrite: bool = False, exclusive: bool = False) -> None:
        """File-based message queue.

        If the queue is exclusive and it is entered multiple times, a RuntimeError will be raised.

        Args:
            queue_path (Path): The path to the queue file.
            overwrite (bool, optional): Whether to overwrite the queue file if it exists. Defaults to False.
            exclusive (bool, optional): Whether to use exclusive access to the queue file. Defaults to False.
        """
        self._messages: list[Message] = []
        self._context_stacklevel = 0
        self._exclusive = exclusive
        self.queue_path = queue_path.resolve()
        super().__init__()
        if queue_path.exists():
            if not queue_path.resolve().is_file():
                raise FileExistsError(f"Queue path {queue_path} already exists and is not a file.")
            if not overwrite:
                raise FileExistsError(f"Queue path {queue_path} already exists, set overwrite=True to ignore this.")
            logger.warning("Queue path already exists, it will be overwritten.", queue_path=queue_path, queue=self)

    def send(self, message: Message) -> None:
        if self._context_stacklevel == 0:
            logger.warning("Using FileQueue outside of a context manager does not persist the messages.", queue=self)
        self._messages.append(message)

    def delete(self, receipt_handle: str) -> None:
        raise NotImplementedError("FileQueue does not implement message deletion.")

    def _flush(self) -> None:
        serializable_list = [
            {"message_type": message.__class__.__name__, "message": message.model_dump_json()}
            for message in self._messages
        ]
        parent = self.queue_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        tmp_path: str | None = None
        try:
            # encoding pinned to utf-8 to match iter_messages_from_file's reader;
            # flush + fsync guarantee the bytes hit disk before the atomic rename.
            with tempfile.NamedTemporaryFile("w", dir=parent, delete=False, encoding="utf-8") as tmp_file:
                tmp_path = tmp_file.name
                json.dump(serializable_list, tmp_file)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, self.queue_path)  # noqa: PTH105 - atomic same-dir replace
        except Exception:
            # delete=False means a failure before the rename would otherwise
            # strand the temp file next to the queue file.
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)
            raise
        logger.info(
            f"Flushed {len(self._messages)} from FileQueue.",
            queue=self,
            messages=len(self._messages),
            queue_path=self.queue_path,
        )
        self._messages.clear()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} at {hex(id(self))} exclusive={self._exclusive} path={self.queue_path}>"

    def __enter__(self) -> Self:
        if self._exclusive and self._context_stacklevel != 0:
            raise RuntimeError(
                "FileQueue's context has already been entered, this cannot be done twice in exclusive mode."
            )
        self._context_stacklevel += 1
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._context_stacklevel -= 1
        if self._context_stacklevel != 0:
            logger.info("FileQueue's context was exited, but there is still an open context somewhere, skipping flush.")
            return
        self._flush()

    @staticmethod
    def iter_messages_from_file(queue_file: Path) -> Iterator[Message]:
        with queue_file.open("r", encoding="utf-8") as file:
            raw_messages = json.load(file)
        for raw_message in raw_messages:
            serialized_message = SerializedMessage.model_validate(raw_message)
            yield Message.deserialize(serialized_message.message, serialized_message.message_type)
