"""LookML generator utilities for type mapping and column name handling."""

import logging
from typing import Optional

from dbt2lookml.enums import LookerBigQueryDataType
from dbt2lookml.models.dbt import DbtModelColumn


def get_catalog_column_info(column_name: str, catalog_data: dict, model_unique_id: str, original_name: str = None) -> dict:
    """
    Get catalog column information with fallback to original case lookup.
    Returns the catalog column data or None if not found.
    """
    if not catalog_data or not model_unique_id:
        return None
    
    node_columns = catalog_data.get('nodes', {}).get(model_unique_id, {}).get('columns', {})
    if not node_columns:
        return None
    
    # Try lowercase lookup first
    catalog_column = node_columns.get(column_name)
    
    # Try original case if lowercase lookup fails
    if not catalog_column and original_name:
        catalog_column = node_columns.get(original_name)
    
    return catalog_column


def is_single_value_array(catalog_column: dict) -> bool:
    """
    Check if a catalog column represents a single value array (ARRAY<primitive>).
    Returns True for ARRAY<primitive> types, False for ARRAY<STRUCT> types.
    """
    if not catalog_column:
        return False
    
    full_type = catalog_column.get('type', '')
    return full_type.startswith('ARRAY<') and 'STRUCT' not in full_type


def get_array_element_looker_type(catalog_column: dict) -> str:
    """
    Get the appropriate Looker type for an array element based on its BigQuery type.
    Returns 'string', 'number', or 'yesno'.
    """
    if not catalog_column:
        return 'string'
    
    full_type = catalog_column.get('type', '')
    if full_type.startswith('ARRAY<') and 'STRUCT' not in full_type:
        # Extract element type from ARRAY<TYPE>
        element_type = full_type[6:-1]  # Remove ARRAY< and >
        if element_type in ['INT64', 'INTEGER', 'NUMERIC', 'FLOAT64', 'FLOAT']:
            return 'number'
        elif element_type in ['BOOL', 'BOOLEAN']:
            return 'yesno'
    
    return 'string'


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


def get_column_name(column: DbtModelColumn, table_format_sql: bool = True, catalog_data: dict = None, model_unique_id: str = None, is_nested_view: bool = False, array_model_name: str = None) -> str:
    """Get the appropriate column name for SQL references.
    
    Args:
        column: The column object
        table_format_sql: Whether to format as ${TABLE}.column_name (always True now)
        catalog_data: Raw catalog data for dynamic type analysis
        
    Returns:
        Formatted column name for SQL reference with ${TABLE} prefix
    """
    def quote_column_name_if_needed(col_name: str) -> str:
        """Quote column name with backticks if it contains spaces, special characters, or non-ASCII characters."""
        # Check for spaces, special characters, or non-ASCII characters (like Swedish ä, ö, å)
        if (' ' in col_name or 
            any(char in col_name for char in ['-', '+', '/', '*', '(', ')', '[', ']']) or
            not col_name.isascii()):
            return f"`{col_name}`"
        return col_name
    
    def is_struct_field(parent_path: str, catalog_data: dict, model_unique_id: str) -> bool:
        """Check if a parent field is a STRUCT by looking up its type in catalog data."""
        if not catalog_data or not model_unique_id:
            return False
            
        # Look up the parent field's type in raw catalog data
        nodes = catalog_data.get('nodes', {})
        if model_unique_id in nodes:
            columns = nodes[model_unique_id].get('columns', {})
            if parent_path in columns:
                parent_type = columns[parent_path].get('type', '')
                # Check if it's a STRUCT (not ARRAY<STRUCT>)
                return 'STRUCT<' in parent_type and not parent_type.startswith('ARRAY<')
        return False
    
    def get_field_type(field_path: str, catalog_data: dict, model_unique_id: str) -> str:
        """Get the data type of a field from catalog data."""
        if not catalog_data or not model_unique_id:
            return ''
            
        nodes = catalog_data.get('nodes', {})
        if model_unique_id in nodes:
            columns = nodes[model_unique_id].get('columns', {})
            if field_path in columns:
                return columns[field_path].get('type', '')
        return ''

    def analyze_nested_field_pattern(column_name: str, catalog_data: dict = None, model_unique_id: str = None) -> tuple:
        """Analyze field pattern to determine the correct SQL syntax.
        
        Handles complex patterns like ARRAY_STRUCT_ARRAY by analyzing the full hierarchy.
        
        Returns:
            tuple: (pattern_type, parent_path)
            pattern_type: 'struct_parent', 'array_child', or 'simple'
            parent_path: the parent path to use in SQL (if applicable)
        """
        if '.' not in column_name:
            return 'simple', None
            
        parts = column_name.split('.')
        
        # Try catalog data analysis first if available
        if catalog_data and model_unique_id:
            # Check immediate parent type first (highest priority)
            immediate_parent_path = '.'.join(parts[:-1])
            immediate_parent_type = get_field_type(immediate_parent_path, catalog_data, model_unique_id)
            
            if immediate_parent_type.startswith('ARRAY<STRUCT<'):
                return 'array_child', None
            elif 'STRUCT<' in immediate_parent_type and not immediate_parent_type.startswith('ARRAY<'):
                return 'struct_parent', immediate_parent_path
            
            # Check for higher-level STRUCT parents if immediate parent isn't definitive
            for i in range(len(parts) - 2, 0, -1):  # Work backwards from second-to-last
                parent_path = '.'.join(parts[:i+1])
                parent_type = get_field_type(parent_path, catalog_data, model_unique_id)
                if is_struct_field(parent_path, catalog_data, model_unique_id):
                    return 'struct_parent', parent_path
        
        # If catalog data is unavailable, return simple pattern
        # This should rarely happen since catalog data should always be available
        return 'simple', None
    
    
    # Use original_name from column if available (preserves catalog case)
    if hasattr(column, 'original_name') and column.original_name:
        original_name = column.original_name
        
        # For nested views, strip the array model prefix from SQL references
        if is_nested_view and '.' in original_name and array_model_name:
            parts = original_name.split('.')
            array_model_parts = array_model_name.split('.')
            
            # Handle case-insensitive matching for array model prefix
            # Strip array model prefix from nested field paths
            if len(parts) > len(array_model_parts):
                # Check if the first parts match the array model (case-insensitive)
                parts_match = True
                for i, array_part in enumerate(array_model_parts):
                    if i < len(parts) and parts[i].lower() != array_part.lower():
                        parts_match = False
                        break
                
                if parts_match:
                    # Remove the array model prefix parts
                    remaining_parts = parts[len(array_model_parts):]
                    struct_path = '.'.join(remaining_parts)
                    quoted_name = quote_column_name_if_needed(struct_path)
                else:
                    quoted_name = quote_column_name_if_needed(original_name)
            elif len(parts) == len(array_model_parts) + 1:
                # Simple field in array: ArrayName.FieldName -> FieldName
                if parts[0].lower() == array_model_parts[0].lower():
                    field_name = parts[-1]
                    quoted_name = quote_column_name_if_needed(field_name)
                else:
                    quoted_name = quote_column_name_if_needed(original_name)
            else:
                quoted_name = quote_column_name_if_needed(original_name)
        elif is_nested_view and '.' in original_name:
            # Fallback for when array_model_name is not provided - use full path
            quoted_name = quote_column_name_if_needed(original_name)
        else:
            quoted_name = quote_column_name_if_needed(original_name)
        
        return f"${{TABLE}}.{quoted_name}"
    
    # Fallback to column name with ${TABLE} prefix
    column_name = column.name
    quoted_name = quote_column_name_if_needed(column_name)
    return f"${{TABLE}}.{quoted_name}"
