import datetime
import json
from pathlib import Path
from typing import Any

import fastavro

_TEST_FILES = ("users",)


def _parse_datetimes(row: dict[str, Any]) -> dict[str, Any]:
    return {
        column: datetime.datetime.fromisoformat(value) if column == "date" else value for column, value in row.items()
    }


def create_test_data() -> None:
    data_dir = Path(__file__).parent
    for test_file in _TEST_FILES:
        test_file_path = data_dir / f"assets/{test_file}.json"
        test_file_schema_path = data_dir / f"assets/{test_file}.v1.avro.json"
        test_file_avro_path = data_dir / f"assets/{test_file}.v1.avro"
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


if __name__ == "__main__":
    create_test_data()
