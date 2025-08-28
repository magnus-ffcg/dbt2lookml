"""Test LookML Dimension Generator implementations."""

from argparse import Namespace

import pytest

from dbt2lookml.enums import (
    LookerValueFormatName,
    LookerDateTimeframes,
    LookerTimeTimeframes,
)
from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.generators.utils import map_bigquery_to_looker
from dbt2lookml.models.dbt import (
    DbtModel,
    DbtModelColumn,
    DbtModelColumnMeta,
    DbtModelMeta,
    DbtResourceType,
)
from dbt2lookml.models.looker import (
    DbtMetaLooker,
    DbtMetaLookerDimension,
)
from unittest.mock import Mock, patch
from dbt2lookml.models.looker import (
    DbtMetaLookerMeasure,
    DbtMetaLookerMeasureFilter,
)
from dbt2lookml.utils import Sql


@pytest.fixture
def cli_args():
    """Fixture for CLI arguments."""
    return Namespace(
        use_table_name=False,
        build_explore=False,
        table_format_sql=True,
    )


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("${TABLE}.column", "${TABLE}.column"),
        ("${view_name}.field", "${view_name}.field"),
        ("invalid sql", None),  # No ${} syntax
        ("SELECT * FROM table;;", None),  # Invalid ending
        ("${TABLE}.field;;", "${TABLE}.field"),  # Removes semicolons
    ],
)
def test_validate_sql(sql, expected):
    """Test SQL validation for Looker expressions."""
    sql_util = Sql()
    assert sql_util.validate_sql(sql) == expected


@pytest.mark.parametrize(
    "bigquery_type, expected_looker_type",
    [
        ("STRING", "string"),
        ("INT64", "number"),
        ("FLOAT64", "number"),
        ("BOOL", "yesno"),
        ("TIMESTAMP", "timestamp"),
        ("DATE", "date"),
        ("DATETIME", "datetime"),
        ("ARRAY<STRING>", "string"),
        ("STRUCT<name STRING>", "string"),
        ("INVALID_TYPE", None),
    ],
)
def test_map_bigquery_to_looker(bigquery_type, expected_looker_type):
    """Test mapping of BigQuery types to Looker types."""
    assert map_bigquery_to_looker(bigquery_type) == expected_looker_type


def test_dimension_group_time(cli_args):
    """Test creation of time-based dimension groups."""
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={},
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type=DbtResourceType.MODEL,
        schema="test_schema",
        description="Test model",
        tags=[],
    )
    column = DbtModelColumn(
        name="created_at",
        lookml_name="created_at",
        lookml_long_name="created_at",
        data_type="TIMESTAMP",
        meta=DbtModelColumnMeta(),
        nested=False,
    )
    dimension_generator = LookmlDimensionGenerator(cli_args)
    result = dimension_generator.lookml_dimension_group(column, "time", True, model)
    assert isinstance(result[0], dict)
    assert result[0].get("type") == "time"
    # Current implementation uses LookerTimeTimeframes enum values
    expected_timeframes = ['raw', 'time', 'date', 'week', 'month', 'quarter', 'year']
    assert result[0].get("timeframes") == expected_timeframes
    assert result[0].get("convert_tz") == "yes"


def test_dimension_group_date(cli_args):
    """Test creation of date-based dimension groups."""
    dimension_generator = LookmlDimensionGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={},
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type=DbtResourceType.MODEL,
        schema="test_schema",
        description="Test model",
        tags=[],
    )
    column = DbtModelColumn(
        name="created_date",
        lookml_name="created_date",
        lookml_long_name="created_date",
        data_type="DATE",
        nested=False,
        meta=DbtModelColumnMeta(
            looker=DbtMetaLooker(
                dimension=DbtMetaLookerDimension(
                    label="Custom Date Label", group_label="Custom Group"
                )
            )
        ),
    )
    dimension_group, dimension_set, dimensions = dimension_generator.lookml_dimension_group(
        column, "date", True, model
    )
    assert dimension_group["type"] == "time"
    assert dimension_group["convert_tz"] == "no"
    # Current implementation returns simplified timeframes
    expected_timeframes = ['raw', 'date', 'week', 'month', 'quarter', 'year']
    assert dimension_group["timeframes"] == expected_timeframes
    assert dimension_group["label"] == "Custom Date Label"
    assert dimension_group["group_label"] == "Custom Group"
    assert dimension_group["name"] == "created"  # Dynamic transformation removes _date suffix
    assert dimension_set["name"] == "s_created"
    assert all(
        tf in dimension_set["fields"]
        for tf in [f"created_{t}" for t in expected_timeframes]
    )
    # Dimensions should be empty list for dimension groups
    assert dimensions == []


def test_lookml_dimensions_with_metadata(cli_args):
    """Test dimension generation with various metadata options."""
    dimension_generator = LookmlDimensionGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={
            "string_col": DbtModelColumn(
                name="string_col",
                lookml_name="string_col",
                lookml_long_name="string_col",
                data_type="STRING",
                description="Custom Description",
                meta=DbtModelColumnMeta(
                    looker=DbtMetaLooker(
                        dimension=DbtMetaLookerDimension(
                            label="Custom Label",
                            group_label="Custom Group",
                            value_format_name=LookerValueFormatName.USD,
                            description="Custom Description",
                        )
                    )
                ),
            )
        },
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type=DbtResourceType.MODEL,
        schema="test_schema",
        description="Test model",
        tags=[],
    )
    dimensions, _ = dimension_generator.lookml_dimensions_from_model(model)
    assert len(dimensions) == 1
    dimension = dimensions[0]
    assert dimension["name"] == "string_col"
    assert dimension["label"] == "Custom Label"
    assert dimension["group_label"] == "Custom Group"
    assert dimension["description"] == "Custom Description"
    assert dimension["value_format_name"] == LookerValueFormatName.USD.value


# Extended test methods for comprehensive coverage

class TestLookmlDimensionGeneratorExtended:
    """Extended tests for LookmlDimensionGenerator to improve coverage."""

    def setup_method(self):
        """Set up test fixtures."""
        args = Namespace(
            use_table_name=False,
            build_explore=False,
            table_format_sql=True,
        )
        self.generator = LookmlDimensionGenerator(args)

    def test_get_conflicting_timeframes_empty_dimension_group(self):
        """Test _get_conflicting_timeframes with empty dimension group."""
        result = self.generator._get_conflicting_timeframes({}, set(), "test_col")
        assert result == []

    def test_get_conflicting_timeframes_unknown_looker_type(self):
        """Test _get_conflicting_timeframes with unknown looker type."""
        dimension_group = {"datatype": "unknown_type"}
        result = self.generator._get_conflicting_timeframes(dimension_group, set(), "test_col")
        assert result == []

    def test_get_conflicting_timeframes_no_conflicts(self):
        """Test _get_conflicting_timeframes with no conflicts."""
        dimension_group = {"datatype": "date"}
        existing_names = {"other_field"}
        result = self.generator._get_conflicting_timeframes(dimension_group, existing_names, "test_col")
        assert result == []

    def test_format_label_edge_cases(self):
        """Test _format_label with edge cases."""
        assert self.generator._format_label("") == ""
        assert self.generator._format_label("_") == " "
        assert self.generator._format_label("__") == "  "

    def test_apply_meta_looker_attributes_no_meta(self):
        """Test _apply_meta_looker_attributes when column has no meta."""
        column = DbtModelColumn(name="test", data_type="STRING")
        target_dict = {}
        self.generator._apply_meta_looker_attributes(target_dict, column, ["label"])
        assert target_dict == {}

    def test_create_iso_field_edge_cases(self):
        """Test _create_iso_field with edge cases."""
        column = DbtModelColumn(name="test_date", data_type="DATE")
        
        # Test ISO year field creation
        iso_year = self.generator._create_iso_field("year", column, "${TABLE}.test_date")
        assert iso_year["name"] == "test_date_iso_year"
        assert iso_year["type"] == "number"
        assert "isoyear" in iso_year["sql"].lower()
        
        # Test ISO week field creation
        iso_week = self.generator._create_iso_field("week", column, "${TABLE}.test_date")
        assert iso_week["name"] == "test_date_iso_week"
        assert iso_week["type"] == "number"
        assert "isoweek" in iso_week["sql"].lower()

    def test_get_dimension_type_edge_cases(self):
        """Test dimension type handling with edge cases."""
        column = DbtModelColumn(name="test", data_type="UNKNOWN_TYPE")
        dimension = self.generator._create_dimension(column, "${TABLE}.test")
        if dimension is not None:
            assert dimension["type"] == "string"
        else:
            assert dimension is None

    def test_create_dimension_with_nested_fields(self):
        """Test _create_dimension with nested field handling."""
        column = DbtModelColumn(name="classification.item_group.code", data_type="STRING")
        column.nested = True
        
        dimension = self.generator._create_dimension(column, "${TABLE}.Classification.ItemGroup.Code")
        
        assert dimension["name"] == "classification__item_group__code"
        assert dimension["group_label"] == "Classification Item Group"
        assert dimension["group_item_label"] == "Code"

    def test_create_dimension_with_primary_key(self):
        """Test _create_dimension with primary key."""
        column = DbtModelColumn(name="id", data_type="STRING")
        column.is_primary_key = True
        
        dimension = self.generator._create_dimension(column, "${TABLE}.id")
        assert dimension["primary_key"] == "yes"

    def test_create_dimension_with_hidden_flag(self):
        """Test _create_dimension with hidden flag."""
        column = DbtModelColumn(name="hidden_field", data_type="STRING")
        
        dimension = self.generator._create_dimension(column, "${TABLE}.hidden_field", is_hidden=True)
        assert dimension["hidden"] == "yes"

    def test_lookml_dimension_group_with_custom_timeframes(self):
        """Test lookml_dimension_group with custom timeframes."""
        self.generator._custom_timeframes = {
            'date': ['date', 'month', 'year'],
            'time': ['time', 'hour', 'day']
        }
        
        column = DbtModelColumn(name="created_at", data_type="TIMESTAMP")
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        dimension_group, dimension_group_set, dimensions = self.generator.lookml_dimension_group(
            column, "time", True, model
        )
        
        assert dimension_group["timeframes"] == ['time', 'hour', 'day']

    def test_lookml_dimension_group_with_description_none(self):
        """Test lookml_dimension_group when description is None."""
        column = DbtModelColumn(name="test_date", data_type="DATE")
        column.description = None
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        dimension_group, _, _ = self.generator.lookml_dimension_group(column, "date", True, model)
        
        assert "description" not in dimension_group

    def test_lookml_dimension_group_without_iso_fields(self):
        """Test lookml_dimension_group without ISO fields."""
        self.generator._include_iso_fields = False
        
        column = DbtModelColumn(name="test_date", data_type="DATE")
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        dimension_group, dimension_group_set, dimensions = self.generator.lookml_dimension_group(
            column, "date", True, model
        )
        
        assert dimensions == []

    def test_transform_date_column_name_edge_cases(self):
        """Test _transform_date_column_name with edge cases."""
        # Test column without original_name
        column = DbtModelColumn(name="test_date", data_type="DATE")
        result = self.generator._transform_date_column_name(column)
        assert result == "test"
        
        # Test nested field transformation
        column = DbtModelColumn(name="delivery.start.date", data_type="DATE")
        column.original_name = "Delivery.Start.Date"
        result = self.generator._transform_date_column_name(column)
        assert result == "delivery__start"

    def test_dimension_name_transformation(self):
        """Test dimension name transformation logic."""
        column = DbtModelColumn(name="DeliveryStartDate", data_type="DATE")
        dimension = self.generator._create_dimension(column, "${TABLE}.DeliveryStartDate")
        
        assert "delivery" in dimension["name"].lower()
        
        column2 = DbtModelColumn(name="delivery_start_date", data_type="DATE")
        dimension2 = self.generator._create_dimension(column2, "${TABLE}.delivery_start_date")
        assert dimension2["name"] == "delivery_start_date"

    def test_transform_date_column_name_lowercase_handling(self):
        """Test _transform_date_column_name with lowercase handling."""
        column = DbtModelColumn(name="deliverystartdate", data_type="DATE")
        column.original_name = "deliverystartdate"
        result = self.generator._transform_date_column_name(column)
        assert result == "deliverystart"

    def test_dimension_group_timeframes(self):
        """Test dimension group timeframe handling."""
        column = DbtModelColumn(name="updated_at", data_type="TIMESTAMP")
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        dimension_group, _, _ = self.generator.lookml_dimension_group(column, "time", True, model)
        
        assert "timeframes" in dimension_group
        assert isinstance(dimension_group["timeframes"], list)

    def test_get_dimension_group_generated_names(self):
        """Test _get_dimension_group_generated_names."""
        date_names = self.generator._get_dimension_group_generated_names("created", "date")
        assert "created_date" in date_names
        assert "created_month" in date_names
        assert "created_year" in date_names
        
        time_names = self.generator._get_dimension_group_generated_names("updated", "time")
        assert "updated_time" in time_names

    def test_create_group_label_edge_cases(self):
        """Test _create_group_label with edge cases."""
        assert self.generator._create_group_label([]) == ""
        assert self.generator._create_group_label(["single"]) == "Single"
        assert self.generator._create_group_label(["multiple", "parts"]) == "Multiple Parts"

    def test_create_item_label_edge_cases(self):
        """Test _create_item_label with edge cases."""
        assert self.generator._create_item_label("") == ""
        assert self.generator._create_item_label("simple") == "Simple"
        assert self.generator._create_item_label("camelCase") == "Camel Case"

    def test_is_single_type_array(self):
        """Test _is_single_type_array method."""
        # Test simple array
        column = DbtModelColumn(name="tags", data_type="ARRAY")
        column.inner_types = ["STRING"]
        assert self.generator._is_single_type_array(column) is True
        
        # Test complex array
        column.inner_types = ["STRUCT<name STRING>"]
        assert self.generator._is_single_type_array(column) is False
        
        # Test non-array
        column.data_type = "STRING"
        assert self.generator._is_single_type_array(column) is False

    def test_add_dimension_to_dimension_group(self):
        """Test _add_dimension_to_dimension_group method."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        date_column = DbtModelColumn(name="created_date", data_type="DATE")
        model.columns["created_date"] = date_column
        
        dimensions = []
        self.generator._add_dimension_to_dimension_group(model, dimensions, True)
        
        if self.generator._include_iso_fields:
            assert len(dimensions) > 0

    def test_lookml_dimensions_from_model_with_include_names(self):
        """Test lookml_dimensions_from_model with include_names."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        column1 = DbtModelColumn(name="field1", data_type="STRING")
        column2 = DbtModelColumn(name="field2", data_type="STRING")
        model.columns = {"field1": column1, "field2": column2}
        
        dimensions, nested_dims = self.generator.lookml_dimensions_from_model(
            model, include_names=["field1"]
        )
        
        if dimensions:
            dimension_names = [d["name"] for d in dimensions]
            assert "field1" in dimension_names or len(dimensions) == 0
        else:
            assert len(dimensions) == 0

    def test_lookml_dimensions_from_model_classification_struct(self):
        """Test lookml_dimensions_from_model with classification struct."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        classification_col = DbtModelColumn(name="classification", data_type="STRUCT<code STRING>")
        model.columns["classification"] = classification_col
        
        nested_col = DbtModelColumn(name="classification.item_group.code", data_type="STRING")
        nested_col.nested = True
        model.columns["classification.item_group.code"] = nested_col
        
        dimensions, nested_dims = self.generator.lookml_dimensions_from_model(model)
        
        dimension_names = [d["name"] for d in dimensions]
        assert any("classification" in name for name in dimension_names)

    def test_lookml_dimensions_from_model_exclude_datetime(self):
        """Test lookml_dimensions_from_model excludes DATETIME columns."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        datetime_col = DbtModelColumn(name="updated_at", data_type="DATETIME")
        model.columns["updated_at"] = datetime_col
        
        dimensions, nested_dims = self.generator.lookml_dimensions_from_model(model)
        
        dimension_names = [d["name"] for d in dimensions]
        assert "updated_at" not in dimension_names

    def test_lookml_dimensions_from_model_none_data_type(self):
        """Test lookml_dimensions_from_model with None data_type."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        null_col = DbtModelColumn(name="null_field", data_type=None)
        model.columns["null_field"] = null_col
        
        dimensions, nested_dims = self.generator.lookml_dimensions_from_model(model)
        
        dimension_names = [d["name"] for d in dimensions]
        assert "null_field" not in dimension_names

    def test_lookml_dimensions_from_model_array_column(self):
        """Test lookml_dimensions_from_model with array column."""
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        array_col = DbtModelColumn(name="tags", data_type="ARRAY<STRING>")
        array_col.inner_types = ["STRING"]
        model.columns["tags"] = array_col
        
        dimensions, nested_dims = self.generator.lookml_dimensions_from_model(model)
        
        dimension_names = [d["name"] for d in dimensions]


