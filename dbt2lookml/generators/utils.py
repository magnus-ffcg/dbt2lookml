"""LookML generator utilities."""
from dbt2lookml.enums import LookerBigQueryDataType
from dbt2lookml.models.dbt import DbtModelColumn

def map_bigquery_to_looker(column_type: str) -> str:
    """Map BigQuery data type to Looker data type."""
    if column_type:
        column_type = column_type.split('<')[0]  # STRUCT< or ARRAY<
        column_type = column_type.split('(')[0]  # Numeric(1,31)

    try:
        return LookerBigQueryDataType.get(column_type)
    except ValueError:
        return None
    
def get_column_name(column: DbtModelColumn, table_format_sql: bool) -> str:
    """Get name of column."""
    if not table_format_sql and '.' in column.name:
        return column.lookml_name

    if '.' in column.name:
        # For nested fields in the main view, include the parent path
        parent_path = '.'.join(column.name.split('.')[:-1])
        return f'${{{parent_path}}}.{column.lookml_name}'

    return f'${{TABLE}}.{column.name}'