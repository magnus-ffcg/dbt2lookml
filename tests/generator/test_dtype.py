from dbt2looker_bigquery.generator import map_bigquery_to_looker

# Mock for logging to test if warning is logged correctly
from unittest.mock import patch

def test_map_adapter_type_to_looker_valid_input():
    column_type = 'INT64'
    expected_output = 'number'
    assert map_bigquery_to_looker(column_type) == expected_output

def test_map_adapter_type_to_looker_unsupported_column():
    column_type = 'UNSUPPORTED_TYPE'
    assert map_bigquery_to_looker(column_type) is None