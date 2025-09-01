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


def get_column_name(column: DbtModelColumn, table_format_sql: bool = True, catalog_data: dict = None, model_unique_id: str = None) -> str:
    """Get the appropriate column name for SQL references.
    
    Args:
        column: The column object
        table_format_sql: Whether to format as ${TABLE}.column_name
        catalog_data: Raw catalog data for dynamic type analysis
        
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
        if table_format_sql:
            quoted_name = quote_column_name_if_needed(column.original_name)
            return f"${{TABLE}}.{quoted_name}"
        else:
            # For nested views, determine SQL syntax based on catalog data analysis
            field_name = column.original_name.split('.')[-1]
            
            # Dynamic analysis using catalog data to determine STRUCT vs ARRAY patterns
            field_name = column.original_name.split('.')[-1]
            # Use provided model_unique_id or try to get it from column
            if not model_unique_id:
                model_unique_id = getattr(column, 'model_unique_id', None)
            
            # Use catalog data to determine the correct SQL syntax
            pattern, struct_parent_path = analyze_nested_field_pattern(column.original_name, catalog_data, model_unique_id)
            
            if pattern == 'struct_parent' and struct_parent_path:
                # STRUCT parent: use ${TABLE}.parent.field syntax (only immediate parent)
                parts = column.original_name.split('.')
                struct_parts = struct_parent_path.split('.')
                
                # Build path from immediate STRUCT parent onwards (not full path)
                nested_path = '.'.join(parts[len(struct_parts):])
                immediate_parent = struct_parts[-1]  # Only the immediate parent
                quoted_name = quote_column_name_if_needed(f"{immediate_parent}.{nested_path}")
                return f"${{TABLE}}.{quoted_name}"
            elif pattern == 'array_child':
                # ARRAY child: check nesting depth for ${TABLE} format
                nesting_levels = column.original_name.count('.')
                if nesting_levels >= 2:
                    # Deep nesting: use ${TABLE}.field format
                    quoted_field = quote_column_name_if_needed(field_name)
                    return f"${{TABLE}}.{quoted_field}"
                else:
                    # Simple ARRAY child: use simple field syntax
                    return quote_column_name_if_needed(field_name)
            else:
                # Simple field: use simple field syntax
                return quote_column_name_if_needed(field_name)
    
    # Fallback to column name
    column_name = column.name.split('.')[-1] if not table_format_sql else column.name
    quoted_name = quote_column_name_if_needed(column_name)
    
    if table_format_sql:
        return f"${{TABLE}}.{quoted_name}"
    else:
        return quoted_name
