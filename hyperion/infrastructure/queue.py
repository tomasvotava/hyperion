import abc
import datetime
from enum import Enum
from typing import ClassVar

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
    def deserialize(json_str: str, message_type: str) -> "Message":
        return Message._subclasses[message_type].model_validate_json(json_str)

    created: datetime.datetime = Field(default_factory=lambda: datetime.datetime.now(tz=datetime.timezone.utc))
    sender: str = config.service_name


class DataLakeArrivalMessage(Message):
    asset: DataLakeAsset
    event: ArrivalEvent


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

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class InMemoryQueue(Queue):
    def __init__(self) -> None:
        super().__init__()
        self._messages: list[Message] = []

    def send(self, message: Message) -> None:
        self._messages.append(message)


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
