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
    assert get_column_name(column, False) == '${TABLE}.simple_column'


def test_get_column_name_nested():
    """Test getting column name for nested columns."""
    column = DbtModelColumn(name='parent.child', data_type='STRING')

    # Test with table_format_sql=True
    assert get_column_name(column, True) == '${parent}.child'

    # Test with table_format_sql=False
    assert get_column_name(column, False) == 'child'


def test_get_column_name_deeply_nested():
    """Test getting column name for deeply nested columns."""
    column = DbtModelColumn(name='grandparent.parent.child', data_type='STRING')

    # Test with table_format_sql=True
    assert get_column_name(column, True) == '${grandparent.parent}.child'

    # Test with table_format_sql=False
    assert get_column_name(column, False) == 'child'
    

def test_hash_uniqueness():
    """Test that empty/invalid inputs generate unique hash-based names."""
    
    # Test empty string generates expected hash-based name
    result_empty = safe_name("")
    assert result_empty == "error_d41d8cd9", f"Expected 'error_d41d8cd9' for empty string, got {repr(result_empty)}"
    
    # Test whitespace-only string generates expected hash-based name  
    result_spaces = safe_name("   ")
    assert result_spaces.startswith("error_"), f"Expected name starting with 'error_' for spaces, got {repr(result_spaces)}"
    assert len(result_spaces) == 14, f"Expected 14 characters (error_ + 8 char hash), got {len(result_spaces)}"
    
    # Test symbol-only string generates expected hash-based name
    result_symbols = safe_name("!!!")
    assert result_symbols.startswith("error_"), f"Expected name starting with 'error_' for symbols, got {repr(result_symbols)}"
    assert len(result_symbols) == 14, f"Expected 14 characters (error_ + 8 char hash), got {len(result_symbols)}"
    
    # Test that each invalid input generates a unique hash
    test_cases = ["", "!!!", "@@@", "###", "   ", "...", "---"]
    results = set()
    
    for test_input in test_cases:
        result = safe_name(test_input)
        
        # Verify expected format: "error_" + 8 character hash
        assert result.startswith("error_"), f"Expected result to start with 'error_', got {repr(result)}"
        assert len(result) == 14, f"Expected 14 total characters, got {len(result)} for input {repr(test_input)}"
        assert result[6:].isalnum(), f"Expected hash part to be alphanumeric, got {repr(result[6:])} for input {repr(test_input)}"
        
        # Verify uniqueness
        assert result not in results, f"Hash collision: {result} generated for multiple inputs"
        results.add(result)
    
    # Verify all inputs generated unique hashes
    assert len(results) == len(test_cases), f"Expected {len(test_cases)} unique hashes, got {len(results)}"
    
    # Test deterministic behavior - same input should always produce same hash
    for _ in range(3):
        assert safe_name("") == "error_d41d8cd9", "Empty string should always produce same hash"
        assert safe_name("!!!") == safe_name("!!!"), "Same input should produce same hash"
