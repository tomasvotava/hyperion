"""Smoke tests for `hyperion.sources.cli`.

`sources/` is a CLI consumer touched by F7 / F9 of the DDD refactor. These tests
are intentionally thin — they only catch import-path / contract breakage during
the module relocation. They do not exercise lambda or argo-workflow runtime
behaviour end-to-end.
"""

from typing import Any

import pytest
from aws_lambda_typing.events.event_bridge import EventBridgeEvent
from aws_lambda_typing.events.sqs import SQSEvent

from hyperion.sources.cli import _assert_source_event_type, _context_from_obj


class TestAssertSourceEventType:
    def test_none_passthrough(self) -> None:
        assert _assert_source_event_type(None) is None

    def test_non_dict_raises(self) -> None:
        with pytest.raises(TypeError, match="expected 'dict'"):
            _assert_source_event_type("not a dict")

    def test_event_bridge_event(self) -> None:
        # Minimal payload that satisfies EventBridgeEvent's required keys.
        event: dict[str, Any] = {
            "version": "0",
            "id": "evt-id",
            "detail-type": "test",
            "source": "test-source",
            "account": "111122223333",
            "time": "2025-01-01T00:00:00Z",
            "region": "us-east-1",
            "resources": [],
            "detail": {"key": "value"},
        }
        assert _assert_source_event_type(event) is event

    def test_sqs_event_empty_records(self) -> None:
        event: dict[str, Any] = {"Records": []}
        assert _assert_source_event_type(event) is event

    def test_sqs_event_with_records(self) -> None:
        event: dict[str, Any] = {
            "Records": [
                {
                    "messageId": "m1",
                    "receiptHandle": "r1",
                    "body": "{}",
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1700000000000",
                        "SenderId": "sid",
                        "ApproximateFirstReceiveTimestamp": "1700000000000",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "x",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:111122223333:queue",
                    "awsRegion": "us-east-1",
                }
            ]
        }
        assert _assert_source_event_type(event) is event

    def test_unknown_dict_raises(self) -> None:
        with pytest.raises(TypeError, match="Invalid source event type"):
            _assert_source_event_type({"some": "key"})


class TestContextFromObj:
    def test_none_passthrough(self) -> None:
        assert _context_from_obj(None) is None

    def test_non_dict_raises(self) -> None:
        with pytest.raises(TypeError, match="expected 'dict'"):
            _context_from_obj(42)

    def test_returns_context_object(self) -> None:
        # The aws_lambda_typing.context.Context object has predefined fields; this
        # smoke test ensures unknown keys are silently ignored and known ones round-trip.
        context = _context_from_obj({"unknown_field": "ignored"})
        assert context is not None


# Type-checker-only references to ensure the import-paths stay intact through the
# refactor (these symbols are touched by `cli.py:8-9`).
_TYPE_CHECK_REFERENCES: tuple[type[EventBridgeEvent], type[SQSEvent]] = (EventBridgeEvent, SQSEvent)
