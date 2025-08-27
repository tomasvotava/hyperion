import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypedDict

import pytest

from hyperion.typeutils import dataclass_asdict, is_typed_dict_instance

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


@dataclass
class SampleDataclass:
    field1: int
    field2: str
    field3: float


class SampleTypedDict(TypedDict):
    field: str
    another_field: str
    optional_field: NotRequired[int]


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


def test_typed_dict_instance_correct() -> None:
    obj = {"field": "foo", "another_field": "bar", "optional_field": 42}
    assert is_typed_dict_instance(obj, SampleTypedDict)


def test_typed_dict_instance_extra() -> None:
    obj = {"field": "foo", "another_field": "bar", "optional_field": 42, "extra": "foobar"}
    assert is_typed_dict_instance(obj, SampleTypedDict)


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Optionals do not work on TypedDicts prior to python 3.11")
def test_typed_dict_optional() -> None:
    obj = {"field": "foo", "another_field": "bar"}
    assert is_typed_dict_instance(obj, SampleTypedDict)


def test_typed_dict_missing() -> None:
    obj = {"field": "foo", "bar": "bar"}
    assert not is_typed_dict_instance(obj, SampleTypedDict)
