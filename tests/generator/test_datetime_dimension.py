import pytest
from dbt2looker_bigquery.models import DbtModelColumn
from dbt2looker_bigquery.generator import NotImplementedError, lookml_dimension_group, looker_time_timeframes, map_bigquery_to_looker

@pytest.fixture
def mock_column():
    return DbtModelColumn(name="test_column", description="Test column", data_type="TIMESTAMP")

def test_lookml_date_time_dimension_group_valid_input(mock_column):
    expected_output = {
        'name': mock_column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{mock_column.name}',
        'description': mock_column.description,
        'datatype': map_bigquery_to_looker(mock_column.data_type),
        'timeframes': looker_time_timeframes,
        'convert_tz': 'yes',
        'group_label': 'Test Column', 
        'label': 'Test Column'
    }
    dimension_group, _, _ = lookml_dimension_group(mock_column, 'time')
    assert dimension_group == expected_output

def test_lookml_date_time_dimension_group_unsupported_data_type(mock_column):
    mock_column.data_type = "UNSUPPORTED_TYPE"
    with pytest.raises(NotImplementedError):
        lookml_dimension_group(mock_column, 'time')