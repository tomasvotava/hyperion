"""Tests for hyperion.data.typeutils and hyperion.data.asset_schemas helpers."""

import pandera.engines.polars_engine as pe
import pandera.polars as pa
import polars
import pytest

from hyperion.data.asset_schemas import PolarsFeatureModel
from hyperion.data.typeutils import map_pandera_dtype_to_polars


class _UnmappedDtype:
    """A dtype that is not in PANDERA_TO_POLARS_MAPPING, used to trigger NotImplementedError."""


class TestMapPanderaDtypeToPolars:
    def test_raises_not_implemented_for_unmapped_instance(self) -> None:
        instance = _UnmappedDtype()
        with pytest.raises(NotImplementedError, match=repr(instance)):
            map_pandera_dtype_to_polars(instance)  # type: ignore[arg-type]

    def test_raises_not_implemented_for_unmapped_class(self) -> None:
        with pytest.raises(NotImplementedError, match=repr(_UnmappedDtype)):
            map_pandera_dtype_to_polars(_UnmappedDtype)  # type: ignore[arg-type]

    def test_error_message_does_not_contain_bare_type_string(self) -> None:
        """The error must show the dtype repr, not the unhelpful literal string `<class 'type'>`."""
        instance = _UnmappedDtype()
        with pytest.raises(NotImplementedError) as exc_info:
            map_pandera_dtype_to_polars(instance)  # type: ignore[arg-type]
        assert "<class 'type'>" not in str(exc_info.value)

    def test_mapped_dtype_int64(self) -> None:
        result = map_pandera_dtype_to_polars(pe.Int64)
        assert result is polars.Int64

    def test_mapped_dtype_string(self) -> None:
        result = map_pandera_dtype_to_polars(pe.String)
        assert result is polars.String


class TestPolarsFeatureModelToPolarsSchemaDefinition:
    def test_simple_model_keys_match_schema_column_names(self) -> None:
        class SimpleModel(PolarsFeatureModel):
            value: pe.Int64

        result = SimpleModel.to_polars_schema_definition()
        assert list(result.keys()) == list(SimpleModel.to_schema().columns.keys())

    def test_aliased_column_key_is_schema_name_not_attribute_name(self) -> None:
        """The dict key must be the schema-defined column name (alias), not the Python attribute name."""

        class AliasedModel(PolarsFeatureModel):
            my_attr: pe.String = pa.Field(alias="actual_col_name")

        result = AliasedModel.to_polars_schema_definition()
        assert "actual_col_name" in result
        assert "my_attr" not in result

    def test_returns_correct_polars_dtype(self) -> None:
        class TypedModel(PolarsFeatureModel):
            count: pe.Int32
            label: pe.String

        result = TypedModel.to_polars_schema_definition()
        assert result["count"] is polars.Int32
        assert result["label"] is polars.String
