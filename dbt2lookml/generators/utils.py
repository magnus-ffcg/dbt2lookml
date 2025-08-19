"""LookML generator utilities for type mapping and column name handling."""

import logging
from typing import Optional

from dbt2lookml.enums import LookerBigQueryDataType
from dbt2lookml.models.dbt import DbtModelColumn


def safe_name(name: str) -> str:
    """Convert a name to a safe identifier for Looker.
    
    Only allows alphanumeric characters and underscores [0-9A-Za-z_].
    Uses unidecode to transliterate Unicode characters to ASCII equivalents.
    
    Args:
        name: The input name to make safe
        
    Returns:
        A safe name containing only valid characters
        
    Examples:
        >>> safe_name("My Field Name")
        'My_Field_Name'
        >>> safe_name("åäö-test@123")
        'aao_test_123'
        >>> safe_name("Москва")
        'Moskva'
        >>> safe_name("")
        'unnamed_d41d8cd9'
    """
    import re
    from unidecode import unidecode
    
    # Convert Unicode to ASCII equivalents
    safe = unidecode(name)
    
    # Replace common separators with underscores
    safe = re.sub(r'[ \-\.@]+', '_', safe)
    
    # Remove any remaining invalid characters (keep only [0-9A-Za-z_])
    safe = re.sub(r'[^0-9A-Za-z_]', '_', safe)
    
    # Clean up multiple consecutive underscores
    safe = re.sub(r'_+', '_', safe)
    
    # Remove leading/trailing underscores
    safe = safe.strip('_')
    
    # Ensure we don't return an empty string
    if not safe:
        import hashlib
        # Create a unique identifier based on the original input
        hash_suffix = hashlib.md5(name.encode('utf-8')).hexdigest()[:8]
        safe = f"error_{hash_suffix}"
    
    return safe


def map_bigquery_to_looker(column_type: Optional[str]) -> Optional[str]:
    """Map BigQuery data type to Looker data type.

    Args:
        column_type: BigQuery data type to map, can be None

    Returns:
        Mapped Looker type, or None if type is invalid or unmappable

    Examples:
        >>> map_bigquery_to_looker('STRING')
        'string'
        >>> map_bigquery_to_looker('INT64')
        'number'
        >>> map_bigquery_to_looker('STRUCT<int64>')
        'string'
    """
    if not column_type:
        return None

    # Strip type parameters
    base_type = (
        column_type.split('<')[0]  # STRUCT< or ARRAY<
        .split('(')[0]  # NUMERIC(10,2)
        .strip()
        .upper()
    )

    try:
        return LookerBigQueryDataType.get(base_type)
    except ValueError:
        logging.warning(f"Unknown BigQuery type: {column_type}")
        return None


def get_column_name(column: DbtModelColumn, table_format_sql: bool) -> str:
    """Get the LookML-formatted name for a column.

    Args:
        column: The DBT model column to format
        table_format_sql: If True, use ${TABLE} syntax for non-nested fields

    Returns:
        LookML-formatted column name

    Examples:
        >>> get_column_name(DbtModelColumn(name='col'), True)
        '${TABLE}.col'
        >>> get_column_name(DbtModelColumn(name='parent.child'), True)
        '${parent}.child'
        >>> get_column_name(DbtModelColumn(name='parent.child'), False)
        'child'
    """
    if not table_format_sql and '.' in column.name:
        assert column.lookml_name is not None, "lookml_name must not be None"
        return column.lookml_name  # Validated in model to never be blank

    if '.' in column.name:
        # For nested fields in the main view, include the parent path
        parent_path = '.'.join(column.name.split('.')[:-1])
        assert column.lookml_name is not None, "lookml_name must not be None"
        return f'${{{parent_path}}}.{column.lookml_name}'

    return f'${{TABLE}}.{column.name}'
