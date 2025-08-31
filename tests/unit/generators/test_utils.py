from dbt2lookml.generators.utils import get_column_name, map_bigquery_to_looker, safe_name
from dbt2lookml.models.dbt import DbtModelColumn


def test_map_bigquery_to_looker_basic_types():
    """Test mapping of basic BigQuery types to Looker types."""
    assert map_bigquery_to_looker('STRING') == 'string'
    assert map_bigquery_to_looker('INT64') == 'number'
    assert map_bigquery_to_looker('FLOAT64') == 'number'
    assert map_bigquery_to_looker('BOOL') == 'yesno'
    assert map_bigquery_to_looker('DATE') == 'date'
    assert map_bigquery_to_looker('DATETIME') == 'datetime'
    assert map_bigquery_to_looker('TIMESTAMP') == 'timestamp'


def test_map_bigquery_to_looker_complex_types():
    """Test mapping of complex BigQuery types to Looker types."""
    # Test STRUCT and ARRAY types (should strip everything after <)
    assert map_bigquery_to_looker('STRUCT<int64>') == 'string'
    assert map_bigquery_to_looker('ARRAY<string>') == 'string'
    # Test NUMERIC/DECIMAL with precision
    assert map_bigquery_to_looker('NUMERIC(10,2)') == 'number'
    assert map_bigquery_to_looker('DECIMAL(5,2)') == 'number'


def test_map_bigquery_to_looker_edge_cases():
    """Test edge cases for BigQuery type mapping."""
    # Test None input
    assert map_bigquery_to_looker(None) is None
    # Test unknown type
    assert map_bigquery_to_looker('UNKNOWN_TYPE') is None
    # Test empty string
    assert map_bigquery_to_looker('') is None


def test_get_column_name_simple():
    """Test getting column name for simple columns."""
    column = DbtModelColumn(name='simple_column', data_type='STRING')
    # Test with table_format_sql=True
    assert get_column_name(column, True) == '${TABLE}.simple_column'
    # Test with table_format_sql=False
    assert get_column_name(column, False) == 'simple_column'


def test_get_column_name_nested():
    """Test getting column name for nested columns."""
    column = DbtModelColumn(name='parent.child', data_type='STRING')
    # Test with table_format_sql=True
    assert get_column_name(column, True) == '${TABLE}.parent.child'
    # Test with table_format_sql=False
    assert get_column_name(column, False) == 'parent.child'


def test_get_column_name_deeply_nested():
    """Test getting column name for deeply nested columns."""
    column = DbtModelColumn(name='grandparent.parent.child', data_type='STRING')
    # Test with table_format_sql=True
    assert get_column_name(column, True) == '${TABLE}.grandparent.parent.child'
    # Test with table_format_sql=False
    assert get_column_name(column, False) == 'grandparent.parent.child'


def test_hash_uniqueness():
    """Test that empty/invalid inputs generate unique hash-based names."""
    # Test empty string generates expected hash-based name
    result_empty = safe_name("")
    assert (
        result_empty == "error_d41d8cd9"
    ), f"Expected 'error_d41d8cd9' for empty string, got {repr(result_empty)}"
    # Test whitespace-only string generates expected hash-based name
    result_spaces = safe_name("   ")
    assert result_spaces.startswith(
        "error_"
    ), f"Expected name starting with 'error_' for spaces, got {repr(result_spaces)}"
    assert (
        len(result_spaces) == 14
    ), f"Expected 14 characters (error_ + 8 char hash), got {len(result_spaces)}"
    # Test symbol-only string generates expected hash-based name
    result_symbols = safe_name("!!!")
    assert result_symbols.startswith(
        "error_"
    ), f"Expected name starting with 'error_' for symbols, got {repr(result_symbols)}"
    assert (
        len(result_symbols) == 14
    ), f"Expected 14 characters (error_ + 8 char hash), got {len(result_symbols)}"
    # Test that each invalid input generates a unique hash
    invalid_cases = ["", "!!!", "@@@", "###", "   ", "---"]  # Removed "..." as dots are valid
    results = set()
    for test_input in invalid_cases:
        result = safe_name(test_input)
        # Verify expected format: "error_" + 8 character hash
        assert result.startswith(
            "error_"
        ), f"Expected result to start with 'error_', got {repr(result)}"
        assert (
            len(result) == 14
        ), f"Expected 14 total characters, got {len(result)} for input {repr(test_input)}"
        assert result[6:].isalnum(), (
            f"Expected hash part to be alphanumeric, got {repr(result[6:])} "
            f"for input {repr(test_input)}"
        )
        # Verify uniqueness
        results.add(result)
    
    # Test that dots are preserved (valid characters)
    assert safe_name("...") == "..."
    # Verify all inputs generated unique hashes
    assert len(results) == len(
        invalid_cases
    ), f"Expected {len(invalid_cases)} unique hashes, got {len(results)}"
    # Test deterministic behavior - same input should always produce same hash
    for _ in range(3):
        assert safe_name("") == "error_d41d8cd9", "Empty string should always produce same hash"
        assert safe_name("!!!") == safe_name("!!!"), "Same input should produce same hash"


# Extended test methods for comprehensive coverage

def test_safe_name_unicode_handling():
    """Test safe_name with Unicode characters."""
    # Test Unicode transliteration
    assert safe_name("åäö") == "aao"
    assert safe_name("Москва") == "Moskva"
    assert safe_name("café") == "cafe"
    
    # Test complex Unicode with separators
    assert safe_name("åäö-test@123") == "aao_test_123"
    assert safe_name("Москва Test") == "Moskva_Test"


def test_safe_name_separator_handling():
    """Test safe_name with various separators."""
    # Test space, dash, @ replacement
    assert safe_name("My Field Name") == "My_Field_Name"
    assert safe_name("test-field") == "test_field"
    assert safe_name("email@domain") == "email_domain"
    assert safe_name("multi   spaces") == "multi_spaces"
    
    # Test multiple consecutive underscores
    assert safe_name("test___field") == "test__field"
    assert safe_name("test____field") == "test__field"


def test_safe_name_invalid_characters():
    """Test safe_name with invalid characters."""
    # Test removal of invalid characters (multiple invalid chars become double underscore)
    assert safe_name("test!@#$%field") == "test__field"
    assert safe_name("field(with)parens") == "field_with_parens"
    assert safe_name("field[with]brackets") == "field_with_brackets"
    
    # Test leading/trailing underscores removal
    assert safe_name("_test_") == "test"
    assert safe_name("__test__") == "test"


def test_safe_name_edge_cases():
    """Test safe_name edge cases that generate error hashes."""
    # Test empty string (should hit line 46-52)
    result = safe_name("")
    assert result == "error_d41d8cd9"
    
    # Test string that becomes empty after cleaning
    result = safe_name("!!!")
    assert result.startswith("error_")
    assert len(result) == 14
    
    # Test whitespace only
    result = safe_name("   ")
    assert result.startswith("error_")
    assert len(result) == 14


def test_map_bigquery_to_looker_exception_handling():
    """Test that unknown types are handled gracefully."""
    # The function catches ValueError from enum lookup and returns None
    result = map_bigquery_to_looker('UNKNOWN_TYPE')
    assert result is None
    
    # Test that it doesn't crash on various invalid inputs
    assert map_bigquery_to_looker('INVALID') is None
    assert map_bigquery_to_looker('NOT_A_TYPE') is None


def test_map_bigquery_to_looker_case_insensitive():
    """Test that type mapping is case insensitive."""
    assert map_bigquery_to_looker('string') == 'string'
    assert map_bigquery_to_looker('String') == 'string'
    assert map_bigquery_to_looker('INT64') == 'number'
    assert map_bigquery_to_looker('int64') == 'number'


def test_get_column_name_with_original_name():
    """Test get_column_name with original_name attribute."""
    # Test column with original_name
    column = DbtModelColumn(name='test_column', data_type='STRING')
    column.original_name = 'TestColumn'
    
    # With table format
    assert get_column_name(column, True) == '${TABLE}.TestColumn'
    # Without table format
    assert get_column_name(column, False) == 'TestColumn'


def test_get_column_name_nested_array_special_cases():
    """Test get_column_name for nested array elements."""
    # Test nested array with 'code' field
    column = DbtModelColumn(name='markings.marking.code', data_type='ARRAY<STRING>')
    column.nested = True
    result = get_column_name(column, True)
    assert result == '${TABLE}.code'
    
    # Test nested array with 'description' field
    column = DbtModelColumn(name='markings.marking.description', data_type='ARRAY<STRING>')
    column.nested = True
    result = get_column_name(column, True)
    assert result == '${TABLE}.description'
    
    # Test nested array with other field
    column = DbtModelColumn(name='markings.marking.other', data_type='ARRAY<STRING>')
    column.nested = True
    result = get_column_name(column, True)
    assert result == '${TABLE}.other'


def test_get_column_name_fallback_without_table_format():
    """Test get_column_name fallback behavior without table format."""
    # Test column without original_name, table_format_sql=False
    column = DbtModelColumn(name='parent.child.field', data_type='STRING')
    result = get_column_name(column, False)
    # Should return the full name when no original_name is set
    assert result == 'parent.child.field'


def test_get_column_name_non_array_nested():
    """Test get_column_name for nested non-array columns."""
    # Test deeply nested non-array column (should not hit special array logic)
    column = DbtModelColumn(name='parent.child.field', data_type='STRING')
    column.nested = True
    result = get_column_name(column, True)
    assert result == '${TABLE}.parent.child.field'


def test_get_column_name_array_without_nested_flag():
    """Test get_column_name for array columns without nested flag."""
    # Test array column without nested=True (should still hit special logic due to ARRAY type)
    column = DbtModelColumn(name='markings.marking.code', data_type='ARRAY<STRING>')
    # nested is False by default, but the function checks for ARRAY in data_type
    result = get_column_name(column, True)
    assert result == '${TABLE}.code'


def test_get_column_name_short_nested_array():
    """Test get_column_name for short nested array (less than 3 parts)."""
    # Test nested array with only 2 parts (should not hit special logic)
    column = DbtModelColumn(name='parent.child', data_type='ARRAY<STRING>')
    column.nested = True
    result = get_column_name(column, True)
    assert result == '${TABLE}.parent.child'


def test_map_bigquery_to_looker_complex_type_stripping():
    """Test complex type parameter stripping."""
    # Test STRUCT with complex nested types
    assert map_bigquery_to_looker('STRUCT<name STRING, age INT64>') == 'string'
    
    # Test ARRAY with nested STRUCT
    assert map_bigquery_to_looker('ARRAY<STRUCT<id INT64>>') == 'string'
    
    # Test NUMERIC with precision and scale
    assert map_bigquery_to_looker('NUMERIC(38,9)') == 'number'
    assert map_bigquery_to_looker('DECIMAL(10,2)') == 'number'
    
    # Test with extra whitespace
    assert map_bigquery_to_looker(' STRING ') == 'string'
    assert map_bigquery_to_looker('  STRUCT<test>  ') == 'string'
