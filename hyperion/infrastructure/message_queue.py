import abc
import datetime
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Iterator
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from aws_lambda_typing.events import SQSEvent
from boto3 import client

# After a long consideration, I decided to use pydantic to serialize / deserialize the messages.
# The convenience outweighs the performance hit.
from pydantic import BaseModel, Field

from hyperion.config import config, queue_config
from hyperion.dateutils import utcnow
from hyperion.entities.catalog import DataLakeAsset
from hyperion.log import get_logger

if sys.version_info >= (3, 11) and TYPE_CHECKING:
    from typing import Self

if sys.version_info < (3, 11) and TYPE_CHECKING:
    from typing_extensions import Self

logger = get_logger("hyperion-queue")


class ArrivalEvent(str, Enum):
    ARRIVED = "arrived"


class Message(BaseModel):
    _subclasses: ClassVar[dict[str, type["Message"]]] = {}

    def __init_subclass__(cls: type["Message"]) -> None:
        cls._subclasses[cls.__name__] = cls

    @staticmethod
    def deserialize(json_str: str, message_type: str, receipt_handle: str | None = None) -> "Message":
        msg = Message._subclasses[message_type].model_validate_json(json_str)
        if receipt_handle and not msg.receipt_handle:
            msg.receipt_handle = receipt_handle
        return msg

    created: datetime.datetime = Field(default_factory=utcnow)
    sender: str = config.service_name
    receipt_handle: str | None = None


class DataLakeArrivalMessage(Message):
    asset: DataLakeAsset
    event: ArrivalEvent
    schema_path: str | None = None


class SourceBackfillMessage(Message):
    source: str
    start_date: datetime.datetime | datetime.date
    end_date: datetime.datetime | datetime.date | None = None
    notify: bool = True


class SerializedMessage(BaseModel):
    message_type: str
    message: str


def iter_messages_from_sqs_event(event: SQSEvent) -> Iterator[Message]:
    if not isinstance(event, dict) or "Records" not in event or not isinstance(event["Records"], list):
        raise ValueError("Provided event is not a valid SQS Event.")
    for record in event["Records"]:
        logger.info("Deserializing message.", message_id=record["messageId"])
        if "MessageType" not in record["messageAttributes"]:
            logger.warning("Message has no type and probably does not come from Hyperion.", message=record)
            continue
        message_type = record["messageAttributes"]["MessageType"]["stringValue"]
        logger.debug("Attempting to deserialize message.", message=record, type=message_type)
        yield Message.deserialize(record["body"], message_type, record["receiptHandle"])


def create_backfill_event(
    message: SourceBackfillMessage, message_id: str = "test", receipt_handle: str = ""
) -> SQSEvent:
    return {
        "Records": [
            {
                "body": message.model_dump_json(),
                "messageId": message_id,
                "receiptHandle": receipt_handle,
                "messageAttributes": {
                    "MessageType": {
                        "binaryListValues": [],
                        "binaryValue": None,
                        "dataType": "String",
                        "stringListValues": [],
                        "stringValue": "SourceBackfillMessage",
                    }
                },
            }
        ]
    }


class Queue(abc.ABC):
    _cached_instance: "ClassVar[Queue | None]" = None

    @staticmethod
    def _resolve_type_from_config() -> "type[Queue]":
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
    def _create_from_config(cls) -> "Queue":
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
    def from_config(cls) -> "Queue":
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


class SQSQueue(Queue):
    def __init__(self, queue_url: str) -> None:
        super().__init__()
        self._queue_url = queue_url
        self._client = client("sqs")

    def send(self, message: Message) -> None:
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message.model_dump_json(),
            MessageAttributes={
                "MessageType": {
                    "DataType": "String",
                    "StringValue": message.__class__.__name__,
                }
            },
        )

    def delete(self, receipt_handle: str) -> None:
        self._client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._queue_url}>"


class FileQueue(Queue):
    def __init__(self, queue_path: Path, *, overwrite: bool = False, exclusive: bool = False) -> None:
        """File-based message queue.

        If the queue is exclusive and it is entered multiple times, a RuntimeError will be raised.

        Args:
            queue_path (Path): The path to the queue file.
            overwrite (bool, optional): Whether to overwrite the queue file if it exists. Defaults to False.
            exclusive (bool, optional): Whether to use exclusive access to the queue file. Defaults to False.
        """
        super().__init__()
        self._messages: list[Message] = []
        self._context_stacklevel = 0
        self._exclusive = exclusive
        if queue_path.exists():
            if not queue_path.resolve().is_file():
                raise FileExistsError(f"Queue path {queue_path} already exists and is not a file.")
            if not overwrite:
                raise FileExistsError(f"Queue path {queue_path} already exists, set overwrite=True to ignore this.")
            logger.warning("Queue path already exists, it will be overwritten.", queue_path=queue_path, queue=self)
        self.queue_path = queue_path.resolve()

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
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp_file:
            json.dump(serializable_list, tmp_file)
        shutil.move(tmp_file.name, self.queue_path)
        logger.info(
            f"Flushed {len(self._messages)} from FileQueue.",
            queue=self,
            messages=len(self._messages),
            queue_path=self.queue_path,
        )
        self._messages.clear()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} at {hex(id(self))} exclusive={self._exclusive} path={self.queue_path}>"

    def __enter__(self) -> "Self":
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
