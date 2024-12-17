import abc
import datetime
from collections.abc import Iterator
from enum import Enum
from typing import ClassVar

from aws_lambda_typing.events import SQSEvent
from boto3 import client

# After a long consideration, I decided to use pydantic to serialize / deserialize the messages.
# The convenience outweighs the performance hit.
from pydantic import BaseModel, Field

from hyperion.config import config, queue_config
from hyperion.entities.catalog import DataLakeAsset
from hyperion.logging import get_logger

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

    created: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(tz=datetime.timezone.utc))
    sender: str = config.service_name
    receipt_handle: str | None = None


class DataLakeArrivalMessage(Message):
    asset: DataLakeAsset
    event: ArrivalEvent


class SourceBackfillMessage(Message):
    source: str
    start_date: datetime.datetime | datetime.date
    end_date: datetime.datetime | datetime.date | None = None
    notify: bool = True


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
    @staticmethod
    def from_config() -> "Queue":
        if queue_config.url is None:
            logger.info("Using in-memory queue.")
            return InMemoryQueue()
        logger.info("Using SQS queue.")
        return SQSQueue(queue_config.url)

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
