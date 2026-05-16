"""Domain message models and their SQS-envelope (de)serialization helpers.

Pure domain: depends only on pydantic, the lite core and
:mod:`hyperion.domain.assets`. ``aws_lambda_typing`` is a typing-only
dependency (no boto3), so this module stays lite. Concrete queue adapters
live in ``hyperion.adapters.queue.*``.
"""

import datetime
from collections.abc import Iterator
from enum import Enum
from typing import ClassVar

from aws_lambda_typing.events import SQSEvent

# After a long consideration, I decided to use pydantic to serialize / deserialize the messages.
# The convenience outweighs the performance hit.
from pydantic import BaseModel, Field

from hyperion.config import config
from hyperion.dateutils import utcnow
from hyperion.domain.assets import DataLakeAsset
from hyperion.log import get_logger

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
