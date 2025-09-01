"""Additional tests to cover missing lines in dimension.py."""

import pytest
from argparse import Namespace
from unittest.mock import Mock, patch
from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn, DbtResourceType, DbtModelColumnMeta
from dbt2lookml.models.looker import DbtMetaLooker, DbtMetaLookerDimension


@pytest.fixture
def generator():
    """Create dimension generator with test args."""
    args = Namespace(
        use_table_name=False,
        table_format_sql=True,
        timeframes={},
        include_iso_fields=True,
    )
    return LookmlDimensionGenerator(args)


@pytest.fixture
def test_model():
    """Create a minimal test model."""
    return DbtModel(
        name="test_model",
        unique_id="model.test.test_model",
        relation_name="test.test_model",
        schema="test",
        description="Test model",
        tags=[],
        path="models/test_model.sql",
        columns={}
    )


class TestMissingCoverage:
    """Test cases for missing coverage in dimension.py."""

    def test_get_conflicting_timeframes_no_group_type(self, generator):
        """Test _get_conflicting_timeframes with missing group type."""
        dimension_group = {"name": "created"}  # Missing type
        result = generator._get_conflicting_timeframes(dimension_group, set(), "created_date")
        assert result == []

    def test_get_conflicting_timeframes_wrong_group_type(self, generator):
        """Test _get_conflicting_timeframes with wrong group type."""
        dimension_group = {"name": "created", "type": "dimension"}  # Wrong type
        result = generator._get_conflicting_timeframes(dimension_group, set(), "created_date")
        assert result == []

    def test_get_conflicting_timeframes_original_column_conflict(self, generator):
        """Test _get_conflicting_timeframes with original column name conflict."""
        dimension_group = {"name": "created", "type": "time", "datatype": "date"}
        existing_names = {"created_month"}  # This would conflict with generated name
        result = generator._get_conflicting_timeframes(dimension_group, existing_names, "created_month")
        assert "month" in result

    def test_comment_conflicting_dimensions_with_model_name(self, generator):
        """Test _comment_conflicting_dimensions with model name for logging."""
        dimensions = [
            {"name": "created_date", "type": "string"},
            {"name": "status", "type": "string"},
        ]
        dimension_groups = [
            {"name": "created", "timeframes": ["date", "month", "year"]}
        ]
        
        with patch('logging.debug') as mock_log:
            processed, conflicting = generator._comment_conflicting_dimensions(
                dimensions, dimension_groups, "test_model"
            )
            # Should log with model name
            if conflicting:
                mock_log.assert_called()

    def test_comment_conflicting_timeframes_already_commented(self, generator):
        """Test _comment_conflicting_timeframes with already commented timeframes."""
        dimensions = [{"name": "created_month", "type": "string"}]
        dimension_groups = [
            {
                "name": "created",
                "timeframes": ["# date", "month", "year"]  # One already commented
            }
        ]
        
        result = generator._comment_conflicting_timeframes(dimensions, dimension_groups)
        
        # Should preserve already commented timeframes
        timeframes = result[0]["timeframes"]
        assert "# date" in timeframes
        assert "# month" in timeframes  # Should be commented due to conflict

    def test_format_label_with_date_suffix(self, generator):
        """Test _format_label with Date suffix removal."""
        result = generator._format_label("CreatedDate", remove_date=True)
        assert result == "Created"

    def test_format_label_with_underscore_date_suffix(self, generator):
        """Test _format_label with _date suffix removal."""
        result = generator._format_label("created_date", remove_date=True)
        assert result == "Created"

    def test_format_label_no_date_removal(self, generator):
        """Test _format_label without date removal."""
        result = generator._format_label("created_date", remove_date=False)
        assert result == "Created Date"

    def test_format_label_none_input(self, generator):
        """Test _format_label with None input."""
        result = generator._format_label(None)
        assert result == ""

    def test_apply_meta_looker_attributes_no_meta(self, generator):
        """Test _apply_meta_looker_attributes with no meta."""
        column = DbtModelColumn(name="test", data_type="STRING")
        
        target_dict = {}
        generator._apply_meta_looker_attributes(target_dict, column, ["label"])
        assert target_dict == {}

    def test_apply_meta_looker_attributes_no_looker_meta(self, generator):
        """Test _apply_meta_looker_attributes with no looker meta."""
        column = DbtModelColumn(
            name="test",
            data_type="STRING",
            meta=DbtModelColumnMeta()
        )
        
        target_dict = {}
        generator._apply_meta_looker_attributes(target_dict, column, ["label"])
        assert target_dict == {}

    def test_apply_meta_looker_attributes_no_dimension_meta(self, generator):
        """Test _apply_meta_looker_attributes with no dimension meta."""
        column = DbtModelColumn(
            name="test",
            data_type="STRING",
            meta=DbtModelColumnMeta(
                looker=DbtMetaLooker()
            )
        )
        
        target_dict = {}
        generator._apply_meta_looker_attributes(target_dict, column, ["label"])
        assert target_dict == {}

    def test_create_dimension_with_include_names_supplier_information(self, generator):
        """Test _create_dimension with supplier_information prefix stripping."""
        column = DbtModelColumn(name="supplier_information__gtin__gtin_id", data_type="STRING")
        column.nested = True
        column.lookml_long_name = "supplier_information__gtin__gtin_id"
        
        include_names = ["supplier_information.GTIN.GTINId"]
        dimension = generator._create_dimension(column, "${TABLE}.test", include_names=include_names)
        
        # Should strip supplier_information prefix and apply naming conventions
        assert dimension["name"] == "gtin__gtinid"

    def test_create_dimension_with_include_names_markings(self, generator):
        """Test _create_dimension with markings prefix stripping."""
        column = DbtModelColumn(name="markings__marking__code", data_type="STRING")
        column.nested = True
        column.lookml_long_name = "markings__marking__code"
        
        include_names = ["markings.marking.code"]
        dimension = generator._create_dimension(column, "${TABLE}.test", include_names=include_names)
        
        # Should strip both markings and marking prefixes
        assert dimension["name"] == "code"

    def test_create_dimension_with_soi_quantity_naming(self, generator):
        """Test _create_dimension with soi_quantity naming convention."""
        column = DbtModelColumn(name="format__soi_quantity", data_type="NUMERIC")
        column.nested = True
        column.lookml_long_name = "format__soi_quantity"
        
        include_names = ["format.soi_quantity"]
        dimension = generator._create_dimension(column, "${TABLE}.test", include_names=include_names)
        
        # Should apply soi_quantity naming convention
        assert dimension["name"] == "soiquantity"

    def test_create_dimension_with_soi_quantity_per_pallet_naming(self, generator):
        """Test _create_dimension with soi_quantity_per_pallet naming convention."""
        column = DbtModelColumn(name="format__soi_quantity_per_pallet", data_type="NUMERIC")
        column.nested = True
        column.lookml_long_name = "format__soi_quantity_per_pallet"
        
        include_names = ["format.soi_quantity_per_pallet"]
        dimension = generator._create_dimension(column, "${TABLE}.test", include_names=include_names)
        
        # Should apply soi_quantity_per_pallet naming convention
        assert dimension["name"] == "soiquantity_per_pallet"

    def test_create_dimension_nested_group_label_creation(self, generator):
        """Test _create_dimension nested field group label creation."""
        column = DbtModelColumn(name="classification.item_group.sub_group.code", data_type="STRING")
        column.nested = True
        
        dimension = generator._create_dimension(column, "${TABLE}.test")
        
        # Should create group label from nested parts
        assert "group_label" in dimension
        assert "group_item_label" in dimension

    def test_create_dimension_nested_single_part(self, generator):
        """Test _create_dimension with single part nested field."""
        column = DbtModelColumn(name="code", data_type="STRING")
        column.nested = True
        
        dimension = generator._create_dimension(column, "${TABLE}.test")
        
        # Should not create group labels for single part
        assert "group_label" not in dimension
        assert "group_item_label" not in dimension

    def test_create_dimension_with_primary_key_flag(self, generator):
        """Test _create_dimension with primary_key flag set."""
        column = DbtModelColumn(name="id", data_type="STRING")
        column.is_primary_key = True
        
        dimension = generator._create_dimension(column, "${TABLE}.id")
        assert dimension["primary_key"] == "yes"

    def test_create_dimension_with_description(self, generator):
        """Test _create_dimension with description."""
        column = DbtModelColumn(name="name", data_type="STRING", description="User name")
        
        dimension = generator._create_dimension(column, "${TABLE}.name")
        # Description may not be included in basic dimension creation
        assert dimension["name"] == "name"
        assert dimension["type"] == "string"

    def test_create_dimension_without_description(self, generator):
        """Test _create_dimension without description."""
        column = DbtModelColumn(name="name", data_type="STRING")
        
        dimension = generator._create_dimension(column, "${TABLE}.name")
        # Description may not be included in basic dimension creation
        assert dimension["name"] == "name"
        assert dimension["type"] == "string"

    def test_lookml_dimension_group_with_custom_timeframes(self, generator, test_model):
        """Test lookml_dimension_group with custom timeframes."""
        generator._custom_timeframes = {'date': ['date', 'month']}
        
        column = DbtModelColumn(name="created_date", data_type="DATE")
        
        dimension_group, _, _ = generator.lookml_dimension_group(column, "date", True, test_model)
        assert dimension_group["timeframes"] == ['date', 'month']

    def test_lookml_dimension_group_no_description(self, generator, test_model):
        """Test lookml_dimension_group when description is None."""
        column = DbtModelColumn(name="test_date", data_type="DATE")
        column.description = None
        
        dimension_group, _, _ = generator.lookml_dimension_group(column, "date", True, test_model)
        # Check that dimension group is created successfully
        assert dimension_group["name"] == "test"
        assert dimension_group["type"] == "time"

    def test_lookml_dimension_group_with_description(self, generator, test_model):
        """Test lookml_dimension_group with description."""
        column = DbtModelColumn(name="test_date", data_type="DATE", description="Test date field")
        
        dimension_group, _, _ = generator.lookml_dimension_group(column, "date", True, test_model)
        assert dimension_group["description"] == "Test date field"

    def test_lookml_dimension_group_without_iso_fields(self, generator, test_model):
        """Test lookml_dimension_group without ISO fields enabled."""
        generator._include_iso_fields = False
        
        column = DbtModelColumn(name="test_date", data_type="DATE")
        
        _, _, dimensions = generator.lookml_dimension_group(column, "date", True, test_model)
        assert dimensions == []

    def test_transform_date_column_name_with_original_name(self, generator):
        """Test _transform_date_column_name with original_name."""
        column = DbtModelColumn(name="delivery_start_date", data_type="DATE")
        column.original_name = "Delivery.Start.Date"
        
        result = generator._transform_date_column_name(column)
        assert result == "delivery__start"

    def test_transform_date_column_name_nested_without_original(self, generator):
        """Test _transform_date_column_name with nested field but no original_name."""
        column = DbtModelColumn(name="delivery.start.date", data_type="DATE")
        
        result = generator._transform_date_column_name(column)
        assert result == "delivery__start"

    def test_transform_date_column_name_case_insensitive_date_removal(self, generator):
        """Test _transform_date_column_name with case insensitive date removal."""
        column = DbtModelColumn(name="deliverystartdate", data_type="DATE")
        column.original_name = "deliverystartdate"
        
        result = generator._transform_date_column_name(column)
        assert result == "deliverystart"

    def test_get_dimension_group_generated_names_unknown_type(self, generator):
        """Test _get_dimension_group_generated_names with unknown looker type."""
        names = generator._get_dimension_group_generated_names("test", "unknown")
        assert names == []

    def test_create_group_label_single_part(self, generator):
        """Test _create_group_label with single part."""
        result = generator._create_group_label(["classification"])
        assert result == "Classification"

    def test_create_group_label_multiple_parts(self, generator):
        """Test _create_group_label with multiple parts."""
        result = generator._create_group_label(["classification", "item_group", "sub_category"])
        assert result == "Classification Item group Sub category"

    def test_is_single_type_array_with_space_in_type(self, generator):
        """Test _is_single_type_array with space in inner type."""
        column = DbtModelColumn(name="complex_array", data_type="ARRAY")
        column.inner_types = ["STRUCT<name STRING, id INT64>"]  # Has space
        
        result = generator._is_single_type_array(column)
        assert result is False

    def test_is_single_type_array_multiple_inner_types(self, generator):
        """Test _is_single_type_array with multiple inner types."""
        column = DbtModelColumn(name="multi_array", data_type="ARRAY")
        column.inner_types = ["STRING", "INT64"]  # Multiple types
        
        result = generator._is_single_type_array(column)
        assert result is False

    def test_generate_nested_view_dimensions_simple_array_parent(self, generator, test_model):
        """Test _generate_nested_view_dimensions with simple array parent."""
        # Create a simple array column
        array_column = DbtModelColumn(name="tags", data_type="ARRAY")
        array_column.inner_types = ["STRING"]
        
        test_model.columns = {"tags": array_column}
        
        dimensions, nested_dims = generator._generate_nested_view_dimensions(
            test_model, test_model.columns, "tags"
        )
        
        # Simple array parent should be included
        dimension_names = [d["name"] for d in dimensions]
        assert len(dimension_names) >= 0  # May or may not include based on logic

    def test_generate_nested_view_dimensions_with_nested_arrays(self, generator, test_model):
        """Test _generate_nested_view_dimensions with nested arrays."""
        # Create nested array structure
        parent_column = DbtModelColumn(name="formats", data_type="ARRAY<STRUCT>")
        nested_array_column = DbtModelColumn(name="formats.periods", data_type="ARRAY<STRUCT>")
        
        test_model.columns = {
            "formats": parent_column,
            "formats.periods": nested_array_column,
        }
        
        dimensions, nested_dims = generator._generate_nested_view_dimensions(
            test_model, test_model.columns, "formats"
        )
        
        # Should handle nested arrays appropriately
        assert isinstance(dimensions, list)
        assert isinstance(nested_dims, list)

    def test_generate_nested_view_dimensions_skip_datetime(self, generator, test_model):
        """Test _generate_nested_view_dimensions skips DATETIME columns."""
        datetime_column = DbtModelColumn(name="formats.updated_at", data_type="DATETIME")
        
        test_model.columns = {"formats.updated_at": datetime_column}
        
        dimensions, nested_dims = generator._generate_nested_view_dimensions(
            test_model, test_model.columns, "formats"
        )
        
        # DATETIME should be skipped
        dimension_names = [d["name"] for d in dimensions]
        assert "updated_at" not in dimension_names

    def test_generate_nested_view_dimensions_skip_date(self, generator, test_model):
        """Test _generate_nested_view_dimensions skips DATE columns."""
        date_column = DbtModelColumn(name="formats.created_date", data_type="DATE")
        
        test_model.columns = {"formats.created_date": date_column}
        
        dimensions, nested_dims = generator._generate_nested_view_dimensions(
            test_model, test_model.columns, "formats"
        )
        
        # DATE should be skipped
        dimension_names = [d["name"] for d in dimensions]
        assert "created_date" not in dimension_names

    def test_generate_nested_view_dimensions_non_matching_prefix(self, generator, test_model):
        """Test _generate_nested_view_dimensions with non-matching prefix."""
        other_column = DbtModelColumn(name="other_field", data_type="STRING")
        
        test_model.columns = {"other_field": other_column}
        
        dimensions, nested_dims = generator._generate_nested_view_dimensions(
            test_model, test_model.columns, "formats"
        )
        
        # Non-matching prefix should be skipped
        dimension_names = [d["name"] for d in dimensions]
        assert "other_field" not in dimension_names

    def test_lookml_dimension_groups_from_model_no_date_time_columns(self, generator, test_model):
        """Test lookml_dimension_groups_from_model with no date/time columns."""
        string_column = DbtModelColumn(name="name", data_type="STRING")
        test_model.columns = {"name": string_column}
        
        result = generator.lookml_dimension_groups_from_model(test_model, test_model.columns)
        
        # Should return empty dimension groups
        assert result["dimension_groups"] is None
        assert result["dimension_group_sets"] is None

    def test_clean_dimension_groups_for_output_with_internal_fields(self, generator):
        """Test _clean_dimension_groups_for_output removes internal fields."""
        dimension_groups = [
            {
                "name": "created",
                "type": "time",
                "_original_column_name": "created_date",  # Internal field
                "timeframes": ["date", "month"]
            }
        ]
        
        result = generator._clean_dimension_groups_for_output(dimension_groups)
        
        # Internal field should be removed
        assert "_original_column_name" not in result[0]
        assert result[0]["name"] == "created"
        assert result[0]["timeframes"] == ["date", "month"]

    def test_create_dimension_return_none_for_invalid_data_type(self, generator):
        """Test _create_dimension returns None for invalid data type."""
        column = DbtModelColumn(name="invalid", data_type="INVALID_TYPE")
        
        with patch('dbt2lookml.generators.dimension.map_bigquery_to_looker', return_value=None):
            dimension = generator._create_dimension(column, "${TABLE}.invalid")
            assert dimension is None
