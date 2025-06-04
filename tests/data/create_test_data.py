import datetime
import json
import random
from collections import defaultdict
from pathlib import Path
from typing import Any

import fastavro

from hyperion.dateutils import quantize_datetime

_TEST_FILES = (
    "users",
    "places",
)

_TEST_FEATURE_HOURS = 168

_TEST_DATA_DIR = Path(__file__).parent


def _parse_datetimes(row: dict[str, Any]) -> dict[str, Any]:
    return {
        column: datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.timezone.utc)
        if column == "date"
        else value
        for column, value in row.items()
    }


def create_test_data() -> None:
    for test_file in _TEST_FILES:
        test_file_path = _TEST_DATA_DIR / f"assets/{test_file}.json"
        test_file_schema_path = _TEST_DATA_DIR / f"assets/{test_file}.v1.avro.json"
        test_file_avro_path = _TEST_DATA_DIR / f"assets/{test_file}.v1.avro"
        if not (test_file_path.exists() and test_file_schema_path.exists()):
            raise FileNotFoundError(f"Test files for {test_file!r} do not exist.")
        test_file_data = json.loads(test_file_path.read_text())
        test_file_schema = json.loads(test_file_schema_path.read_text())
        with test_file_avro_path.open("wb") as file:
            fastavro.writer(
                file,
                schema=test_file_schema,
                records=(_parse_datetimes(row) for row in test_file_data),
                validator=True,
                strict=False,
                codec="deflate",
                codec_compression_level=7,
                strict_allow_default=True,
            )


def create_test_feature() -> None:
    daily_data: dict[datetime.datetime, list[dict[str, Any]]] = defaultdict(list)
    anchor_date = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    for hour in range(_TEST_FEATURE_HOURS):
        timestamp = anchor_date + datetime.timedelta(hours=hour)
        daily_data[quantize_datetime(timestamp.replace(hour=0), "1d")].append(
            {"timestamp": timestamp, "category": random.choice(("a", "b", "c")), "value": random.randint(0, 255)}  # noqa: S311
        )
    feature_base_path = _TEST_DATA_DIR / "assets"
    feature_base_path.mkdir(parents=True, exist_ok=True)
    feature_schema_path = _TEST_DATA_DIR / "assets/superfeature.v1.avro.json"
    with feature_schema_path.open() as file:
        feature_schema = json.load(file)
    for day, data in daily_data.items():
        feature_data_path = feature_base_path / f"superfeature.{day.strftime('%Y-%m-%d')}.v1.avro"
        with feature_data_path.open("wb") as file:
            fastavro.writer(
                file,
                schema=feature_schema,
                records=data,
                validator=True,
                strict=False,
                codec="deflate",
                codec_compression_level=7,
                strict_allow_default=True,
            )


if __name__ == "__main__":
    create_test_feature()
    create_test_data()
