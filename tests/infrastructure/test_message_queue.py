"""Tests for `hyperion.infrastructure.message_queue`.

These cover Message envelope serialization, the three Queue impls, and
the env-driven `from_config()` dispatch. They predate (and survive) the
move to `adapters/queue/{memory,filesystem,sqs}.py` in the DDD refactor.
"""

import datetime
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest

from hyperion.config import queue_config
from hyperion.entities.catalog import DataLakeAsset
from hyperion.infrastructure.message_queue import (
    ArrivalEvent,
    DataLakeArrivalMessage,
    FileQueue,
    InMemoryQueue,
    Message,
    Queue,
    SourceBackfillMessage,
    SQSQueue,
    create_backfill_event,
    iter_messages_from_sqs_event,
)

UTC = datetime.UTC


@pytest.fixture(autouse=True)
def _reset_queue_singleton() -> Iterator[None]:
    # Queue.from_config caches its result in a class var. Reset around each test.
    previous = Queue._cached_instance
    Queue._cached_instance = None
    yield
    Queue._cached_instance = previous


@pytest.fixture(autouse=True)
def _isolate_queue_config(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make sure tests start with no queue config bleed from the environment.
    monkeypatch.setattr(queue_config, "url", None)
    monkeypatch.setattr(queue_config, "path", None)
    monkeypatch.setattr(queue_config, "path_overwrite", False)


def _datalake_message() -> DataLakeArrivalMessage:
    asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC))
    return DataLakeArrivalMessage(asset=asset, event=ArrivalEvent.ARRIVED)


class TestMessageSerialization:
    def test_datalake_arrival_roundtrip(self) -> None:
        original = _datalake_message()
        json_str = original.model_dump_json()
        restored = Message.deserialize(json_str, "DataLakeArrivalMessage")
        assert isinstance(restored, DataLakeArrivalMessage)
        assert restored.asset == original.asset
        assert restored.event == ArrivalEvent.ARRIVED
        assert restored.schema_path is None
        assert restored.created == original.created
        assert restored.sender == original.sender

    def test_source_backfill_roundtrip(self) -> None:
        original = SourceBackfillMessage(
            source="my-source",
            start_date=datetime.datetime(2025, 1, 1, tzinfo=UTC),
            end_date=datetime.datetime(2025, 1, 7, tzinfo=UTC),
            notify=False,
        )
        json_str = original.model_dump_json()
        restored = Message.deserialize(json_str, "SourceBackfillMessage")
        assert isinstance(restored, SourceBackfillMessage)
        assert restored.source == "my-source"
        assert restored.start_date == original.start_date
        assert restored.end_date == original.end_date
        assert restored.notify is False

    def test_deserialize_with_receipt_handle(self) -> None:
        original = _datalake_message()
        restored = Message.deserialize(original.model_dump_json(), "DataLakeArrivalMessage", "rcpt-1")
        assert restored.receipt_handle == "rcpt-1"

    def test_deserialize_does_not_overwrite_existing_receipt(self) -> None:
        original = _datalake_message()
        original.receipt_handle = "already-set"
        restored = Message.deserialize(original.model_dump_json(), "DataLakeArrivalMessage", "rcpt-other")
        assert restored.receipt_handle == "already-set"

    def test_unknown_message_type_raises(self) -> None:
        with pytest.raises(KeyError):
            Message.deserialize("{}", "ImaginaryMessage")

    def test_schema_path_persists_through_roundtrip(self) -> None:
        asset = DataLakeAsset("users", datetime.datetime(2025, 1, 2, tzinfo=UTC))
        original = DataLakeArrivalMessage(
            asset=asset, event=ArrivalEvent.ARRIVED, schema_path="s3://schemas/custom.json"
        )
        restored = Message.deserialize(original.model_dump_json(), "DataLakeArrivalMessage")
        assert isinstance(restored, DataLakeArrivalMessage)
        assert restored.schema_path == "s3://schemas/custom.json"


class TestInMemoryQueue:
    def test_send_appends(self) -> None:
        queue = InMemoryQueue()
        message = _datalake_message()
        queue.send(message)
        assert queue._messages == [message]

    def test_delete_by_receipt_handle(self) -> None:
        queue = InMemoryQueue()
        message_a = _datalake_message()
        message_b = SourceBackfillMessage(source="s", start_date=datetime.datetime(2025, 2, 1, tzinfo=UTC))
        queue.send(message_a)
        queue.send(message_b)
        queue.delete(message_a.created.isoformat())
        assert queue._messages == [message_b]

    def test_delete_missing_handle_is_noop(self) -> None:
        queue = InMemoryQueue()
        queue.send(_datalake_message())
        queue.delete("does-not-exist")
        assert len(queue._messages) == 1


class TestFileQueue:
    def test_send_outside_context_does_not_persist(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue = FileQueue(queue_path)
        queue.send(_datalake_message())
        assert not queue_path.exists()

    def test_send_inside_context_flushes_on_exit(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue = FileQueue(queue_path)
        message = _datalake_message()
        with queue:
            queue.send(message)
        assert queue_path.exists()
        raw = json.loads(queue_path.read_text())
        assert isinstance(raw, list)
        assert len(raw) == 1
        assert raw[0]["message_type"] == "DataLakeArrivalMessage"
        # The inner payload is a JSON string of the actual message.
        inner = json.loads(raw[0]["message"])
        assert inner["event"] == ArrivalEvent.ARRIVED.value

    def test_iter_messages_from_file_roundtrip(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue = FileQueue(queue_path)
        message = _datalake_message()
        with queue:
            queue.send(message)
        loaded = list(FileQueue.iter_messages_from_file(queue_path))
        assert len(loaded) == 1
        assert isinstance(loaded[0], DataLakeArrivalMessage)
        assert loaded[0].asset == message.asset

    def test_overwrite_false_with_existing_path_raises(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue_path.write_text("[]")
        with pytest.raises(FileExistsError):
            FileQueue(queue_path, overwrite=False)

    def test_overwrite_true_replaces_existing(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue_path.write_text("stale")
        queue = FileQueue(queue_path, overwrite=True)
        with queue:
            queue.send(_datalake_message())
        # File was replaced with a JSON list.
        assert json.loads(queue_path.read_text())[0]["message_type"] == "DataLakeArrivalMessage"

    def test_directory_at_queue_path_raises(self, tmp_path: Path) -> None:
        (tmp_path / "dir").mkdir()
        with pytest.raises(FileExistsError):
            FileQueue(tmp_path / "dir", overwrite=True)

    def test_exclusive_mode_re_entry_raises(self, tmp_path: Path) -> None:
        queue = FileQueue(tmp_path / "queue.json", exclusive=True)
        with queue, pytest.raises(RuntimeError, match="exclusive mode"):
            queue.__enter__()

    def test_nested_context_only_flushes_at_outermost(self, tmp_path: Path) -> None:
        queue_path = tmp_path / "queue.json"
        queue = FileQueue(queue_path)
        with queue:
            queue.send(_datalake_message())
            with queue:
                # The inner __exit__ must not write the file yet.
                pass
            assert not queue_path.exists()
        assert queue_path.exists()

    def test_delete_not_implemented(self, tmp_path: Path) -> None:
        queue = FileQueue(tmp_path / "queue.json")
        with pytest.raises(NotImplementedError):
            queue.delete("anything")


class TestSQSQueue:
    @pytest.fixture
    def sqs_queue_url(self, _moto_server: None) -> str:
        client = boto3.client("sqs")
        response = client.create_queue(QueueName="test-queue")
        return str(response["QueueUrl"])

    def test_send_message(self, sqs_queue_url: str) -> None:
        queue = SQSQueue(sqs_queue_url)
        message = _datalake_message()
        queue.send(message)

        client = boto3.client("sqs")
        response = client.receive_message(
            QueueUrl=sqs_queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        assert len(messages) == 1
        assert messages[0]["MessageAttributes"]["MessageType"]["StringValue"] == "DataLakeArrivalMessage"
        body_restored = Message.deserialize(messages[0]["Body"], "DataLakeArrivalMessage")
        assert isinstance(body_restored, DataLakeArrivalMessage)
        assert body_restored.asset == message.asset

    def test_delete_message_via_receipt_handle(self, sqs_queue_url: str) -> None:
        queue = SQSQueue(sqs_queue_url)
        queue.send(_datalake_message())

        client = boto3.client("sqs")
        first = client.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        receipt = first["Messages"][0]["ReceiptHandle"]
        queue.delete(receipt)
        # Subsequent receive should be empty after the visibility timeout (which is 0 here),
        # but moto's behaviour is to return nothing immediately for a deleted message.
        second = client.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        assert "Messages" not in second or not second["Messages"]


class TestQueueFromConfig:
    def test_dispatch_to_in_memory_queue(self) -> None:
        instance = Queue.from_config()
        assert isinstance(instance, InMemoryQueue)

    def test_dispatch_to_file_queue(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        queue_path = tmp_path / "q.json"
        monkeypatch.setattr(queue_config, "path", queue_path.as_posix())
        instance = Queue.from_config()
        assert isinstance(instance, FileQueue)

    @pytest.mark.usefixtures("_moto_server")
    def test_dispatch_to_sqs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(queue_config, "url", "https://sqs.example.com/some-queue")
        instance = Queue.from_config()
        assert isinstance(instance, SQSQueue)

    def test_singleton_cached(self) -> None:
        first = Queue.from_config()
        second = Queue.from_config()
        assert first is second

    def test_cached_inconsistent_with_config_raises(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        # First call resolves to InMemoryQueue.
        first = Queue.from_config()
        assert isinstance(first, InMemoryQueue)

        # Reconfigure to force resolution to a different type; the cached instance
        # no longer matches and from_config must raise.
        monkeypatch.setattr(queue_config, "path", (tmp_path / "q.json").as_posix())
        with pytest.raises(RuntimeError, match="Configuration inconsistency"):
            Queue.from_config()


class TestSQSEventHelpers:
    def test_iter_messages_from_sqs_event_skips_unknown_type(self) -> None:
        event: Any = {
            "Records": [
                {
                    "body": "{}",
                    "messageId": "m1",
                    "receiptHandle": "r1",
                    "messageAttributes": {},
                }
            ]
        }
        assert list(iter_messages_from_sqs_event(event)) == []

    def test_iter_messages_from_sqs_event_rejects_non_event(self) -> None:
        # iter_messages_from_sqs_event is a generator; force the body to run.
        not_an_event: Any = {"NotRecords": []}
        with pytest.raises(ValueError, match="not a valid SQS Event"):
            list(iter_messages_from_sqs_event(not_an_event))

    def test_create_backfill_event_roundtrip(self) -> None:
        message = SourceBackfillMessage(source="my-source", start_date=datetime.datetime(2025, 1, 1, tzinfo=UTC))
        event = create_backfill_event(message, message_id="m-id", receipt_handle="r-id")
        assert event["Records"][0]["messageAttributes"]["MessageType"]["stringValue"] == "SourceBackfillMessage"
        restored = list(iter_messages_from_sqs_event(event))
        assert len(restored) == 1
        assert isinstance(restored[0], SourceBackfillMessage)
        assert restored[0].source == "my-source"
        assert restored[0].receipt_handle == "r-id"
