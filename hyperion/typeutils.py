import datetime
from collections.abc import Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Annotated, Any, TypeAlias, TypeVar

import pandera
import pandera.engines
import pandera.engines.polars_engine
import polars

from hyperion.log import get_logger

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

T = TypeVar("T")

logger = get_logger("hyperion-typeutils")

DateOrDelta: TypeAlias = datetime.datetime | datetime.timedelta
PolarsUTCDateTime = Annotated[pandera.engines.polars_engine.DateTime, False, "UTC", "us"]

PANDERA_TO_POLARS_MAPPING: dict[type[pandera.engines.polars_engine.DataType], type[polars.DataType]] = {
    pandera.engines.polars_engine.Array: polars.Array,
    pandera.engines.polars_engine.Binary: polars.Binary,
    pandera.engines.polars_engine.Bool: polars.Boolean,
    pandera.engines.polars_engine.Categorical: polars.Categorical,
    pandera.engines.polars_engine.Date: polars.Date,
    pandera.engines.polars_engine.DateTime: polars.Datetime,
    pandera.engines.polars_engine.Decimal: polars.Decimal,
    pandera.engines.polars_engine.Enum: polars.Enum,
    pandera.engines.polars_engine.Float32: polars.Float32,
    pandera.engines.polars_engine.Float64: polars.Float64,
    pandera.engines.polars_engine.Int8: polars.Int8,
    pandera.engines.polars_engine.Int16: polars.Int16,
    pandera.engines.polars_engine.Int32: polars.Int32,
    pandera.engines.polars_engine.Int64: polars.Int64,
    pandera.engines.polars_engine.List: polars.List,
    pandera.engines.polars_engine.Null: polars.Null,
    pandera.engines.polars_engine.Object: polars.Object,
    pandera.engines.polars_engine.String: polars.String,
    pandera.engines.polars_engine.Struct: polars.Struct,
    pandera.engines.polars_engine.Time: polars.Time,
    pandera.engines.polars_engine.UInt8: polars.UInt8,
    pandera.engines.polars_engine.UInt16: polars.UInt16,
    pandera.engines.polars_engine.UInt32: polars.UInt32,
    pandera.engines.polars_engine.UInt64: polars.UInt64,
}

POLARS_SCHEMA_COPY_ATTRIBUTES: dict[type[polars.DataType], tuple[tuple[str, Any], ...]] = {
    polars.Datetime: (("time_unit", "us"), ("time_zone", None)),
    polars.Duration: (("time_unit", "us"),),
    polars.Decimal: (("precision", None), ("scale", 0)),
    polars.Categorical: (("ordering", "physical"),),
    polars.Enum: (("categories", None),),
    # TODO: Implement support for recursive types (List, Struct)
    # https://github.com/tomasvotava/hyperion/issues/40
}


def _get_pandera_type_attribute(
    pandera_dtype: type[pandera.engines.polars_engine.DataType] | pandera.engines.polars_engine.DataType,
    attr: str,
    default: T,
) -> T:
    if hasattr(pandera_dtype, "type"):
        return getattr(pandera_dtype.type, attr, default)
    return getattr(pandera_dtype, attr, default)


def map_pandera_dtype_to_polars(
    pandera_dtype: type[pandera.engines.polars_engine.DataType],
) -> type[polars.DataType] | polars.DataType:
    for pandtype, poldtype in PANDERA_TO_POLARS_MAPPING.items():
        if isinstance(pandera_dtype, pandtype):
            if (copy_attributes := POLARS_SCHEMA_COPY_ATTRIBUTES.get(poldtype)) is not None:
                dtype_kwargs = {
                    argname: _get_pandera_type_attribute(pandera_dtype, argname, default)
                    for argname, default in copy_attributes
                }
                return poldtype(**dtype_kwargs)
            if poldtype in (polars.List, polars.Struct):
                logger.warning(
                    "Polars types `List` and `Struct` are not fully supported yet in schema mapping. "
                    "If you're using `List.inner` or `Struct.fields`, and an error occurs during validation, "
                    "this is likely the cause."
                )
            return poldtype
    raise NotImplementedError(f"Mapping for pandera type {type(pandera_dtype)!r} to a polars type does not exist.")


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
