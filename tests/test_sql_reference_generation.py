"""Test cases for SQL reference generation in nested views."""

import pytest
from unittest.mock import Mock
from dbt2lookml.generators.utils import get_column_name
from dbt2lookml.models.dbt import DbtModelColumn


def test_nested_view_sql_reference_stripping():
    """Test that array model prefix is stripped from SQL references in nested views."""
    
    # Mock column for Format.Period.EndDate
    column = Mock()
    column.name = 'format.period.enddate'
    column.original_name = 'Format.Period.EndDate'
    column.data_type = 'DATE'
    
    # Test the SQL reference generation
    result = get_column_name(
        column=column,
        table_format_sql=True,
        is_nested_view=True,
        array_model_name='format'
    )
    
    # Should strip "Format." prefix and result in "${TABLE}.Period.EndDate"
    expected = '${TABLE}.Period.EndDate'
    print(f"Testing: {column.original_name} with array_model_name='format'")
    print(f"Expected: {expected}")
    print(f"Got: {result}")
    
    assert result == expected, f"Expected '{expected}', got '{result}'"


def test_simple_field_sql_reference():
    """Test SQL reference for simple fields in nested views."""
    
    column = Mock()
    column.name = 'format.formatid'
    column.original_name = 'Format.FormatId'
    column.data_type = 'STRING'
    
    result = get_column_name(
        column=column,
        table_format_sql=True,
        catalog_data=None,
        model_unique_id=None,
        is_nested_view=True,
        array_model_name='format'
    )
    
    expected = '${TABLE}.FormatId'
    print(f"Testing: {column.original_name} with array_model_name='format'")
    print(f"Expected: {expected}")
    print(f"Got: {result}")
    
    assert result == expected, f"Expected '{expected}', got '{result}'"


if __name__ == '__main__':
    test_nested_view_sql_reference_stripping()
    test_simple_field_sql_reference()
    print("All tests passed!")
