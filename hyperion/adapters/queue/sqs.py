"""SQS-backed :class:`Queue` adapter (requires boto3 -- ``[aws]``)."""

from __future__ import annotations

from boto3 import client

from hyperion.domain.messages import Message
from hyperion.ports.queue import Queue


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
