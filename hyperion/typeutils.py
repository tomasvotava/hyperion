"""Stdlib type-utility helpers.

.. deprecated::
    The pandera/polars dtype-mapping helpers (:data:`PANDERA_TO_POLARS_MAPPING`,
    :data:`POLARS_SCHEMA_COPY_ATTRIBUTES`, :data:`PolarsUTCDateTime`,
    :func:`map_pandera_dtype_to_polars`) moved to :mod:`hyperion.data.typeutils`
    (F5 / DDD refactor Step 2). Import them from there. This module keeps them
    importable (with a :class:`DeprecationWarning`, resolved lazily so the
    import does not pull the data stack) for the whole ``hyperion-sdk`` 1.x line.
"""

from __future__ import annotations

import datetime
import importlib
from collections.abc import Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, TypeAlias, TypeVar

from hyperion._compat import moved_attr

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

    from hyperion.data.typeutils import (
        PANDERA_TO_POLARS_MAPPING,
        POLARS_SCHEMA_COPY_ATTRIBUTES,
        PolarsUTCDateTime,
        map_pandera_dtype_to_polars,
    )

T = TypeVar("T")

DateOrDelta: TypeAlias = datetime.datetime | datetime.timedelta

_MOVED: dict[str, str] = {
    "PANDERA_TO_POLARS_MAPPING": "hyperion.data.typeutils",
    "POLARS_SCHEMA_COPY_ATTRIBUTES": "hyperion.data.typeutils",
    "PolarsUTCDateTime": "hyperion.data.typeutils",
    "map_pandera_dtype_to_polars": "hyperion.data.typeutils",
}

__all__ = [
    "PANDERA_TO_POLARS_MAPPING",
    "POLARS_SCHEMA_COPY_ATTRIBUTES",
    "DateOrDelta",
    "PolarsUTCDateTime",
    "TypedDictProtocol",
    "assert_type",
    "dataclass_asdict",
    "is_typed_dict_instance",
    "map_pandera_dtype_to_polars",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        new_module = _MOVED[name]
        # Deferred import: the data stack is only pulled in when a consumer
        # actually touches a relocated symbol via the deprecated path.
        module = importlib.import_module(new_module)
        return moved_attr(
            name=name, value=getattr(module, name), old_module="hyperion.typeutils", new_module=new_module
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def assert_type(variable: Any, assertion: type[T]) -> T:
    if not isinstance(variable, assertion):
        raise TypeError(f"Expected {assertion}, got {type(variable)}.")
    return variable


def dataclass_asdict(
    dataclass: DataclassInstance,
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


class TypedDictProtocol(Protocol):
    __required_keys__: ClassVar[frozenset[str]]
    __optional_keys__: ClassVar[frozenset[str]]


def is_typed_dict_instance(obj: dict[Any, Any], typed_dict: type[TypedDictProtocol]) -> bool:
    return set(obj.keys()).intersection(typed_dict.__required_keys__) == typed_dict.__required_keys__
