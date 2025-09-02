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
    """Create a safe name for LookML by removing invalid characters and handling Unicode.
    
    This function converts Unicode characters to ASCII equivalents, replaces common
    separators with underscores, removes invalid characters, and ensures the result
    is a valid LookML identifier. Dots are preserved for nested field names.
    
    Args:
        name: The input name to make safe
        
    Returns:
        A safe name suitable for use in LookML
        
    Examples:
        >>> safe_name("My Field Name")
        'My_Field_Name'
        >>> safe_name("field-name@test")
        'field_name_test'
        >>> safe_name("åäö-test@123")
        'aao_test_123'
        >>> safe_name("Москва")
        'Moskva'
        >>> safe_name("")
        'unnamed_d41d8cd9'
    """
    # Convert Unicode to ASCII equivalents
    safe = _transliterate_unicode(name)
    # Replace common separators with underscores
    safe = _replace_separators(safe)
    # Remove invalid characters
    safe = _remove_invalid_characters(safe)
    # Clean up consecutive underscores
    safe = _clean_consecutive_underscores(safe)
    # Remove leading/trailing underscores
    safe = _strip_boundary_underscores(safe)
    # Handle empty results
    return _handle_empty_result(safe, name)


def _transliterate_unicode(name: str) -> str:
    """Convert Unicode characters to ASCII equivalents."""
    from unidecode import unidecode
    return unidecode(name)


def _replace_separators(text: str) -> str:
    """Replace common separators with underscores (preserve dots for nested fields)."""
    import re
    return re.sub(r'[ \-@]+', '_', text)


def _remove_invalid_characters(text: str) -> str:
    """Remove invalid characters, keeping only alphanumeric, underscores, and dots."""
    import re
    return re.sub(r'[^0-9A-Za-z_.]', '_', text)


def _clean_consecutive_underscores(text: str) -> str:
    """Clean up multiple consecutive underscores, preserving double underscores."""
    import re
    # First replace 3+ underscores with double underscores
    text = re.sub(r'_{3,}', '__', text)
    # Then replace single underscores that aren't part of double underscores
    return re.sub(r'(?<!_)_(?!_)', '_', text)


def _strip_boundary_underscores(text: str) -> str:
    """Remove leading and trailing underscores."""
    return text.strip('_')


def _handle_empty_result(safe: str, original_name: str) -> str:
    """Handle empty results by generating error hash."""
    if not safe:
        import hashlib
        hash_suffix = hashlib.md5(original_name.encode('utf-8')).hexdigest()[:8]
        return f"error_{hash_suffix}"
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
        model_unique_id: Unique identifier for the model
        is_nested_view: Whether this is for a nested view
        array_model_name: Name of the array model for prefix stripping
        
    Returns:
        Formatted column name for SQL reference with ${TABLE} prefix
    """
    # Determine the column name to use
    column_name = _get_effective_column_name(column)
    
    # Process nested view column names if applicable
    if is_nested_view and '.' in column_name and array_model_name:
        processed_name = _process_nested_view_column_name(column_name, array_model_name)
    elif is_nested_view and '.' in column_name:
        processed_name = column_name  # Use full path as fallback
    else:
        processed_name = column_name
    
    # Quote the column name if needed and format with ${TABLE} prefix
    quoted_name = _quote_column_name_if_needed(processed_name)
    return f"${{TABLE}}.{quoted_name}"


def _get_effective_column_name(column: DbtModelColumn) -> str:
    """Get the effective column name to use (original_name if available, otherwise name).
    
    Args:
        column: The column object
        
    Returns:
        The effective column name to use
    """
    if hasattr(column, 'original_name') and column.original_name:
        return column.original_name
    return column.name


def _quote_column_name_if_needed(col_name: str) -> str:
    """Quote column name with backticks if it contains spaces, special characters, or non-ASCII characters.
    
    Args:
        col_name: The column name to potentially quote
        
    Returns:
        Quoted column name if needed, otherwise original name
    """
    # Check for spaces, special characters, or non-ASCII characters (like Swedish ä, ö, å)
    if (' ' in col_name or 
        any(char in col_name for char in ['-', '+', '/', '*', '(', ')', '[', ']']) or
        not col_name.isascii()):
        return f"`{col_name}`"
    return col_name


def _process_nested_view_column_name(column_name: str, array_model_name: str) -> str:
    """Process column name for nested views by stripping array model prefix.
    
    Args:
        column_name: The original column name (e.g., 'items.details.name')
        array_model_name: The array model name to strip (e.g., 'items')
        
    Returns:
        Processed column name with prefix stripped (e.g., 'details.name')
    """
    parts = column_name.split('.')
    array_model_parts = array_model_name.split('.')
    
    # Handle case-insensitive matching for array model prefix
    if len(parts) > len(array_model_parts):
        # Check if the first parts match the array model (case-insensitive)
        if _parts_match_array_model(parts, array_model_parts):
            # Remove the array model prefix parts
            remaining_parts = parts[len(array_model_parts):]
            return '.'.join(remaining_parts)
        else:
            return column_name
    elif len(parts) == len(array_model_parts) + 1:
        # Simple field in array: ArrayName.FieldName -> FieldName
        if parts[0].lower() == array_model_parts[0].lower():
            return parts[-1]
        else:
            return column_name
    else:
        return column_name


def _parts_match_array_model(parts: list, array_model_parts: list) -> bool:
    """Check if column parts match array model parts (case-insensitive).
    
    Args:
        parts: Column name parts split by '.'
        array_model_parts: Array model name parts split by '.'
        
    Returns:
        True if parts match array model prefix, False otherwise
    """
    for i, array_part in enumerate(array_model_parts):
        if i >= len(parts) or parts[i].lower() != array_part.lower():
            return False
    return True


def _is_struct_field(parent_path: str, catalog_data: dict, model_unique_id: str) -> bool:
    """Check if a parent field is a STRUCT by looking up its type in catalog data.
    
    Args:
        parent_path: Path to the parent field
        catalog_data: Raw catalog data
        model_unique_id: Unique identifier for the model
        
    Returns:
        True if parent field is a STRUCT, False otherwise
    """
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


def _get_field_type(field_path: str, catalog_data: dict, model_unique_id: str) -> str:
    """Get the data type of a field from catalog data.
    
    Args:
        field_path: Path to the field
        catalog_data: Raw catalog data
        model_unique_id: Unique identifier for the model
        
    Returns:
        Field data type, or empty string if not found
    """
    if not catalog_data or not model_unique_id:
        return ''
        
    nodes = catalog_data.get('nodes', {})
    if model_unique_id in nodes:
        columns = nodes[model_unique_id].get('columns', {})
        if field_path in columns:
            return columns[field_path].get('type', '')
    return ''


def _analyze_nested_field_pattern(column_name: str, catalog_data: dict = None, model_unique_id: str = None) -> tuple:
    """Analyze field pattern to determine the correct SQL syntax.
    
    Handles complex patterns like ARRAY_STRUCT_ARRAY by analyzing the full hierarchy.
    
    Args:
        column_name: The column name to analyze
        catalog_data: Raw catalog data for type analysis
        model_unique_id: Unique identifier for the model
        
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
        immediate_parent_type = _get_field_type(immediate_parent_path, catalog_data, model_unique_id)
        
        if immediate_parent_type.startswith('ARRAY<STRUCT<'):
            return 'array_child', None
        elif 'STRUCT<' in immediate_parent_type and not immediate_parent_type.startswith('ARRAY<'):
            return 'struct_parent', immediate_parent_path
        
        # Check for higher-level STRUCT parents if immediate parent isn't definitive
        for i in range(len(parts) - 2, 0, -1):  # Work backwards from second-to-last
            parent_path = '.'.join(parts[:i+1])
            if _is_struct_field(parent_path, catalog_data, model_unique_id):
                return 'struct_parent', parent_path
    
    # If catalog data is unavailable, return simple pattern
    # This should rarely happen since catalog data should always be available
    return 'simple', None
