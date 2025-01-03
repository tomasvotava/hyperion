from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from hyperion.typeutils import dataclass_asdict


@dataclass
class SampleDataclass:
    field1: int
    field2: str
    field3: float


@pytest.mark.parametrize(
    ("exclude", "include", "expected"),
    [
        (None, None, {"field1": 1, "field2": "test", "field3": 3.14}),
        (("field1"), None, {"field2": "test", "field3": 3.14}),
        (("field1", "field2", "field3"), None, {}),
        (("field1",), ("field3",), {"field3": 3.14}),
        (None, ("field1",), {"field1": 1}),
    ],
)
def test_dataclass_asdict(
    exclude: Sequence[str] | None, include: Sequence[str] | None, expected: dict[str, Any]
) -> None:
    sample = SampleDataclass(field1=1, field2="test", field3=3.14)
    assert dataclass_asdict(sample, exclude=exclude, include=include) == expected


@pytest.mark.parametrize(
    ("exclude", "include", "match_error"),
    [
        (["field1", "field2"], ["field2", "field3"], "Include and exclude cannot overlap."),
        (["field1", "field2"], ["field4"], "Include field not found in dataclass."),
    ],
)
def test_dataclass_asdict_bad_input(
    exclude: Sequence[str] | None, include: Sequence[str] | None, match_error: str
) -> None:
    sample = SampleDataclass(field1=1, field2="test", field3=3.14)
    with pytest.raises(ValueError, match=match_error):
        dataclass_asdict(sample, exclude=exclude, include=include)
