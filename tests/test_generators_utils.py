from dbt2lookml.generators.utils import get_column_name, map_bigquery_to_looker
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
