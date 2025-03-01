import datetime
from collections.abc import Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

T = TypeVar("T")

DateOrDelta: TypeAlias = datetime.datetime | datetime.timedelta


def assert_type(variable: Any, assertion: type[T]) -> T:
    if not isinstance(variable, assertion):
        raise TypeError(f"Expected {assertion}, got {type(variable)}.")
    return variable


def dataclass_asdict(
    dataclass: "DataclassInstance",
    *,
    exclude: Sequence[str] | None = None,
    include: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Convert a dataclass instance to a dictionary.

    Args:
        dataclass (DataclassInstance): Dataclass instance to convert.
        exclude (Sequence[str], optional): Fields to exclude. Defaults to None.
        include (Sequence[str], optional): Fields to include. Defaults to None.

    Returns:
        dict[str, Any]: Converted dictionary.

    Raises:
        ValueError: If include and exclude overlap.
        ValueError: If include field is not found in dataclass.
    """
    exclude = exclude or []
    include = include or []
    dct = asdict(dataclass)
    fields = list(dct.keys())
    if set(include) & set(exclude):
        raise ValueError("Include and exclude cannot overlap.")
    if set(include) - set(fields):
        raise ValueError("Include field not found in dataclass.")
    if exclude or include:
        for field in fields:
            if (include and field not in include) or field in exclude:
                del dct[field]
    return dct
