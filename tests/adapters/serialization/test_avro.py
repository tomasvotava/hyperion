"""Unit tests for the extracted avro serialization adapter (DDD Step 5)."""

import io

import fastavro
import pytest

from hyperion.adapters.serialization.avro import AvroSerializer, AvroStreamWriter

SCHEMA = {
    "type": "record",
    "name": "Sample",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "count", "type": "long"},
    ],
}
RECORDS = [{"id": "a", "count": 1}, {"id": "b", "count": 2}]
METADATA = {"name": "sample", "schema_version": "1"}


@pytest.fixture
def serializer() -> AvroSerializer:
    return AvroSerializer()


def test_write_then_read_round_trips(serializer: AvroSerializer) -> None:
    buffer = io.BytesIO()
    serializer.write(buffer, SCHEMA, RECORDS, METADATA)
    buffer.seek(0)
    assert list(serializer.read(buffer)) == RECORDS


def test_write_embeds_metadata(serializer: AvroSerializer) -> None:
    buffer = io.BytesIO()
    serializer.write(buffer, SCHEMA, RECORDS, METADATA)
    buffer.seek(0)
    reader = fastavro.reader(buffer)
    list(reader)
    decoded = {
        k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
        for k, v in dict(reader.metadata).items()
    }
    assert decoded.get("name") == "sample"
    assert decoded.get("schema_version") == "1"


def test_write_validates_records(serializer: AvroSerializer) -> None:
    buffer = io.BytesIO()
    with pytest.raises(fastavro.validation.ValidationError):
        serializer.write(buffer, SCHEMA, [{"id": None, "count": 1}], METADATA)


def test_streaming_writer_round_trips(serializer: AvroSerializer) -> None:
    buffer = io.BytesIO()
    writer = serializer.streaming_writer(buffer, SCHEMA, METADATA)
    assert isinstance(writer, AvroStreamWriter)
    for record in RECORDS:
        writer.write(record)
    writer.dump()
    buffer.seek(0)
    assert list(serializer.read(buffer)) == RECORDS
