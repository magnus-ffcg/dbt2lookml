"""Comprehensive unit tests for get_column_name function."""

from unittest.mock import Mock

import pytest

from dbt2lookml.generators.utils import get_column_name
from dbt2lookml.models.dbt import DbtModelColumn


class TestGetColumnNameComprehensive:
    """Comprehensive test coverage for get_column_name function."""

    @pytest.fixture
    def simple_column(self):
        """Create simple column fixture."""
        return DbtModelColumn(name='revenue', data_type='NUMERIC', description='Revenue amount')

    @pytest.fixture
    def column_with_original_name(self):
        """Create column with original_name fixture."""
        column = DbtModelColumn(name='revenue', data_type='NUMERIC', description='Revenue amount')
        column.original_name = 'Revenue_Amount'
        return column

    @pytest.fixture
    def nested_column(self):
        """Create nested column fixture."""
        column = DbtModelColumn(name='items.details.name', data_type='STRING', description='Item detail name')
        column.original_name = 'items.details.name'
        return column

    @pytest.fixture
    def sample_catalog_data(self):
        """Create sample catalog data fixture."""
        return {
            'nodes': {
                'model.test.sample_model': {
                    'columns': {
                        'items': {'type': 'ARRAY<STRUCT<details STRUCT<name STRING>>>'},
                        'items.details': {'type': 'STRUCT<name STRING>'},
                        'items.details.name': {'type': 'STRING'},
                        'categories': {'type': 'STRUCT<name STRING>'},
                        'categories.name': {'type': 'STRING'},
                        'simple_array': {'type': 'ARRAY<STRING>'},
                        'complex_field': {'type': 'ARRAY<STRUCT<value STRING>>'},
                    }
                }
            }
        }

    def test_simple_column_without_original_name(self, simple_column):
        """Test simple column without original_name."""
        result = get_column_name(simple_column)
        assert result == '${TABLE}.revenue'

    def test_column_with_original_name(self, column_with_original_name):
        """Test column with original_name."""
        result = get_column_name(column_with_original_name)
        assert result == '${TABLE}.Revenue_Amount'

    def test_column_name_with_spaces_gets_quoted(self):
        """Test column name with spaces gets quoted with backticks."""
        column = DbtModelColumn(name='field with spaces', data_type='STRING')
        column.original_name = 'field with spaces'

        result = get_column_name(column)
        assert result == '${TABLE}.`field with spaces`'

    def test_column_name_with_special_characters_gets_quoted(self):
        """Test column name with special characters gets quoted."""
        column = DbtModelColumn(name='field-with+special/chars', data_type='STRING')
        column.original_name = 'field-with+special/chars'

        result = get_column_name(column)
        assert result == '${TABLE}.`field-with+special/chars`'

    def test_column_name_with_non_ascii_gets_quoted(self):
        """Test column name with non-ASCII characters gets quoted."""
        column = DbtModelColumn(name='field_åäö', data_type='STRING')
        column.original_name = 'field_åäö'

        result = get_column_name(column)
        assert result == '${TABLE}.`field_åäö`'

    def test_normal_column_name_no_quotes(self):
        """Test normal column name doesn't get quoted."""
        column = DbtModelColumn(name='normal_field_name', data_type='STRING')
        column.original_name = 'normal_field_name'

        result = get_column_name(column)
        assert result == '${TABLE}.normal_field_name'

    def test_nested_column_without_catalog_data(self, nested_column):
        """Test nested column without catalog data."""
        result = get_column_name(nested_column)
        assert result == '${TABLE}.items.details.name'

    def test_nested_column_with_catalog_data_struct_parent(self, sample_catalog_data):
        """Test nested column with catalog data showing STRUCT parent."""
        column = DbtModelColumn(name='categories.name', data_type='STRING')
        column.original_name = 'categories.name'

        result = get_column_name(column, catalog_data=sample_catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.categories.name'

    def test_nested_column_with_catalog_data_array_child(self, sample_catalog_data):
        """Test nested column with catalog data showing ARRAY<STRUCT> parent."""
        column = DbtModelColumn(name='items.details.name', data_type='STRING')
        column.original_name = 'items.details.name'

        result = get_column_name(column, catalog_data=sample_catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.items.details.name'

    def test_table_format_sql_false_fallback(self, simple_column):
        """Test table_format_sql=False (should still use ${TABLE} format)."""
        result = get_column_name(simple_column, table_format_sql=False)
        assert result == '${TABLE}.revenue'

    def test_nested_view_with_array_model_name_simple_field(self):
        """Test nested view with array model name for simple field."""
        column = DbtModelColumn(name='items.code', data_type='STRING')
        column.original_name = 'items.code'

        result = get_column_name(column, is_nested_view=True, array_model_name='items')
        assert result == '${TABLE}.code'

    def test_nested_view_with_array_model_name_nested_field(self):
        """Test nested view with array model name for nested field."""
        column = DbtModelColumn(name='items.details.name', data_type='STRING')
        column.original_name = 'items.details.name'

        result = get_column_name(column, is_nested_view=True, array_model_name='items')
        assert result == '${TABLE}.details.name'

    def test_nested_view_with_multi_level_array_model_name(self):
        """Test nested view with multi-level array model name."""
        column = DbtModelColumn(name='items.details.specs.value', data_type='STRING')
        column.original_name = 'items.details.specs.value'

        result = get_column_name(column, is_nested_view=True, array_model_name='items.details')
        assert result == '${TABLE}.specs.value'

    def test_nested_view_case_insensitive_array_model_matching(self):
        """Test nested view with case-insensitive array model matching."""
        column = DbtModelColumn(name='Items.Code', data_type='STRING')
        column.original_name = 'Items.Code'

        result = get_column_name(column, is_nested_view=True, array_model_name='items')
        assert result == '${TABLE}.Code'

    def test_nested_view_no_array_model_name_fallback(self):
        """Test nested view without array model name uses full path."""
        column = DbtModelColumn(name='items.details.name', data_type='STRING')
        column.original_name = 'items.details.name'

        result = get_column_name(column, is_nested_view=True)
        assert result == '${TABLE}.items.details.name'

    def test_nested_view_non_matching_array_model_name(self):
        """Test nested view with non-matching array model name."""
        column = DbtModelColumn(name='categories.name', data_type='STRING')
        column.original_name = 'categories.name'

        result = get_column_name(column, is_nested_view=True, array_model_name='items')
        assert result == '${TABLE}.categories.name'

    def test_nested_view_insufficient_parts_for_stripping(self):
        """Test nested view where column has insufficient parts for stripping."""
        column = DbtModelColumn(name='code', data_type='STRING')
        column.original_name = 'code'

        result = get_column_name(column, is_nested_view=True, array_model_name='items.details')
        assert result == '${TABLE}.code'

    def test_catalog_data_missing_model_id(self, simple_column, sample_catalog_data):
        """Test with catalog data but missing model unique ID."""
        result = get_column_name(simple_column, catalog_data=sample_catalog_data, model_unique_id='model.test.nonexistent')
        assert result == '${TABLE}.revenue'

    def test_catalog_data_missing_columns(self, simple_column):
        """Test with catalog data missing columns section."""
        catalog_data = {'nodes': {'model.test.sample_model': {}}}

        result = get_column_name(simple_column, catalog_data=catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.revenue'

    def test_empty_catalog_data(self, simple_column):
        """Test with empty catalog data."""
        result = get_column_name(simple_column, catalog_data={}, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.revenue'

    def test_none_catalog_data(self, simple_column):
        """Test with None catalog data."""
        result = get_column_name(simple_column, catalog_data=None, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.revenue'

    def test_analyze_nested_field_pattern_simple_field(self, sample_catalog_data):
        """Test analyze_nested_field_pattern with simple field."""
        column = DbtModelColumn(name='revenue', data_type='NUMERIC')
        column.original_name = 'revenue'

        result = get_column_name(column, catalog_data=sample_catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.revenue'

    def test_analyze_nested_field_pattern_struct_parent(self, sample_catalog_data):
        """Test analyze_nested_field_pattern with STRUCT parent."""
        column = DbtModelColumn(name='categories.name', data_type='STRING')
        column.original_name = 'categories.name'

        result = get_column_name(column, catalog_data=sample_catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.categories.name'

    def test_analyze_nested_field_pattern_array_struct_child(self, sample_catalog_data):
        """Test analyze_nested_field_pattern with ARRAY<STRUCT> child."""
        column = DbtModelColumn(name='complex_field.value', data_type='STRING')
        column.original_name = 'complex_field.value'

        # Add the field to catalog data
        sample_catalog_data['nodes']['model.test.sample_model']['columns']['complex_field.value'] = {'type': 'STRING'}

        result = get_column_name(column, catalog_data=sample_catalog_data, model_unique_id='model.test.sample_model')
        assert result == '${TABLE}.complex_field.value'

    def test_column_without_original_name_attribute(self):
        """Test column that doesn't have original_name attribute."""
        column = DbtModelColumn(name='test_field', data_type='STRING')
        # Don't set original_name attribute

        result = get_column_name(column)
        assert result == '${TABLE}.test_field'

    def test_column_with_empty_original_name(self):
        """Test column with empty original_name."""
        column = DbtModelColumn(name='test_field', data_type='STRING')
        column.original_name = ''

        result = get_column_name(column)
        assert result == '${TABLE}.test_field'

    def test_column_with_none_original_name(self):
        """Test column with None original_name."""
        column = DbtModelColumn(name='test_field', data_type='STRING')
        column.original_name = None

        result = get_column_name(column)
        assert result == '${TABLE}.test_field'

    def test_complex_nested_structure_with_quotes(self):
        """Test complex nested structure requiring quotes."""
        column = DbtModelColumn(name='items.details with spaces.name', data_type='STRING')
        column.original_name = 'items.details with spaces.name'

        result = get_column_name(column, is_nested_view=True, array_model_name='items')
        assert result == '${TABLE}.`details with spaces.name`'

    def test_edge_case_single_dot_in_name(self):
        """Test edge case with single dot in name."""
        column = DbtModelColumn(name='field.name', data_type='STRING')
        column.original_name = 'field.name'

        result = get_column_name(column)
        assert result == '${TABLE}.field.name'
