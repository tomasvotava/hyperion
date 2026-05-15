"""Pandera/pydantic feature models for catalog assets.

Split out of :mod:`hyperion.entities.catalog` (F6 / DDD refactor Step 2). The
heavy validation half -- requires the ``[data]`` extra. Asset *identities* live
in the lite :mod:`hyperion.domain.assets`.
"""

from typing import ClassVar

import pandera.polars as pa
import polars
from pydantic import BaseModel

from hyperion.data.typeutils import map_pandera_dtype_to_polars
from hyperion.dateutils import TimeResolution


class FeatureModel(BaseModel):
    """A base class for feature models.

    You may use this base class (along with pydantic's BaseModel) to define type-safe feature models.
    Use with "AssetCollection" to make powerful typed feature collections.
    """

    asset_name: ClassVar[str] = NotImplemented
    resolution: ClassVar[TimeResolution] = NotImplemented
    schema_version: ClassVar[int] = 1


class PolarsFeatureModel(pa.DataFrameModel):
    _asset_name: ClassVar[str] = NotImplemented
    _resolution: ClassVar[TimeResolution] = NotImplemented
    _schema_version: ClassVar[int] = 1

    @classmethod
    def to_polars_schema_definition(cls) -> dict[str, type[polars.DataType] | polars.DataType]:
        return {field.name: map_pandera_dtype_to_polars(field.dtype) for field in cls.to_schema().columns.values()}
