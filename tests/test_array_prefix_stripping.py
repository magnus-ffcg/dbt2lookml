"""Test cases for array model prefix stripping logic in nested views."""

import pytest
from dbt2lookml.generators.utils import get_column_name
from dbt2lookml.models.dbt import DbtModelColumn


class MockColumn:
    """Mock column for testing."""
    def __init__(self, name, original_name, data_type='STRING'):
        self.name = name
        self.original_name = original_name
        self.data_type = data_type


class TestArrayPrefixStripping:
    """Test array model prefix stripping in nested view SQL generation."""
    
    def test_format_period_enddate_stripping(self):
        """Test that Format.Period.EndDate becomes Period.EndDate in format nested view."""
        column = MockColumn(
            name='format.period.enddate',
            original_name='Format.Period.EndDate',
            data_type='DATE'
        )
        
        result = get_column_name(
            column=column,
            table_format_sql=True,
            is_nested_view=True,
            array_model_name='format'
        )
        
        expected = '${TABLE}.Period.EndDate'
        assert result == expected, f"Expected '{expected}', got '{result}'"
    
    def test_simple_array_field_stripping(self):
        """Test that Format.FormatId becomes FormatId in format nested view."""
        column = MockColumn(
            name='format.formatid',
            original_name='Format.FormatId',
            data_type='STRING'
        )
        
        result = get_column_name(
            column=column,
            table_format_sql=True,
            is_nested_view=True,
            array_model_name='format'
        )
        
        expected = '${TABLE}.FormatId'
        assert result == expected, f"Expected '{expected}', got '{result}'"
    
    def test_case_insensitive_matching(self):
        """Test that case differences between array_model_name and original_name work."""
        column = MockColumn(
            name='supplierinformation.gtin.gtinid',
            original_name='SupplierInformation.GTIN.GTINId',
            data_type='STRING'
        )
        
        result = get_column_name(
            column=column,
            table_format_sql=True,
            is_nested_view=True,
            array_model_name='supplierinformation'
        )
        
        expected = '${TABLE}.GTIN.GTINId'
        assert result == expected, f"Expected '{expected}', got '{result}'"
    
    def test_no_prefix_when_not_matching(self):
        """Test that non-matching prefixes don't get stripped."""
        column = MockColumn(
            name='other.field.name',
            original_name='Other.Field.Name',
            data_type='STRING'
        )
        
        result = get_column_name(
            column=column,
            table_format_sql=True,
            is_nested_view=True,
            array_model_name='format'
        )
        
        expected = '${TABLE}.Other.Field.Name'
        assert result == expected, f"Expected '{expected}', got '{result}'"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
