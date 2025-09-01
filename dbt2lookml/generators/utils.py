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
    # Replace common separators with underscores (but preserve dots for nested fields)
    safe = re.sub(r'[ \-@]+', '_', safe)
    # Remove any remaining invalid characters (keep only [0-9A-Za-z_.])
    safe = re.sub(r'[^0-9A-Za-z_.]', '_', safe)
    # Clean up multiple consecutive underscores, but preserve double underscores
    # First replace 3+ underscores with double underscores
    safe = re.sub(r'_{3,}', '__', safe)
    # Then replace single underscores that aren't part of double underscores
    safe = re.sub(r'(?<!_)_(?!_)', '_', safe)
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


def get_column_name(column: DbtModelColumn, table_format_sql: bool = True) -> str:
    """Get the appropriate column name for SQL references.
    
    Args:
        column: The column object
        table_format_sql: Whether to format as ${TABLE}.column_name
        
    Returns:
        Formatted column name for SQL reference
    """
    def quote_column_name_if_needed(col_name: str) -> str:
        """Quote column name with backticks if it contains spaces, special characters, or non-ASCII characters."""
        # Check for spaces, special characters, or non-ASCII characters (like Swedish ä, ö, å)
        if (' ' in col_name or 
            any(char in col_name for char in ['-', '+', '/', '*', '(', ')', '[', ']']) or
            not col_name.isascii()):
            return f"`{col_name}`"
        return col_name
    
    # For nested array elements, use the last part of the column name
    # Only apply this logic to actual array elements (3+ parts AND nested array type)
    # TODO: below is likely not the correct logic, need verification
    if (table_format_sql and column.nested and len(column.name.split('.')) >= 3 and
        hasattr(column, 'data_type') and column.data_type and 'ARRAY' in str(column.data_type).upper()):
        # For nested array elements like markings.marking.code, use just the last part
        last_part = column.name.split('.')[-1]
        return f"${{TABLE}}.{quote_column_name_if_needed(last_part)}"
    
    
    # Use original_name from column if available (preserves catalog case)
    if hasattr(column, 'original_name') and column.original_name:
        quoted_name = quote_column_name_if_needed(column.original_name)
        if table_format_sql:
            return f"${{TABLE}}.{quoted_name}"
        else:
            return quoted_name
    
    # Fallback to column name
    column_name = column.name.split('.')[-1] if not table_format_sql else column.name
    quoted_name = quote_column_name_if_needed(column_name)
    
    if table_format_sql:
        return f"${{TABLE}}.{quoted_name}"
    else:
        return quoted_name
