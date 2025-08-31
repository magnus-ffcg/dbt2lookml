"""Column collections for organizing model columns by their intended use."""

from typing import Dict, List, Set
from dataclasses import dataclass
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


@dataclass
class ColumnCollections:
    """Pre-structured column collections to avoid filtering during generation."""
    
    main_view_columns: Dict[str, DbtModelColumn]
    nested_view_columns: Dict[str, Dict[str, DbtModelColumn]]  # array_name -> columns
    excluded_columns: Set[str]  # For reference/debugging
    
    @classmethod
    def from_model(cls, model: DbtModel, array_models: List) -> 'ColumnCollections':
        """Create column collections from a model and its array models."""
        main_view_columns = {}
        nested_view_columns = {}
        excluded_columns = set()
        
        # Build hierarchy map for understanding relationships
        hierarchy = cls._build_hierarchy_map(model.columns)
        
        # Collect array model names for easy lookup
        array_model_names = {array_model.name for array_model in array_models}
        
        # First, add array parents to their own nested collections
        for array_model in array_models:
            array_name = array_model.name
            if array_name not in nested_view_columns:
                nested_view_columns[array_name] = {}
            # Include the array parent itself
            if array_name in model.columns:
                nested_view_columns[array_name][array_name] = model.columns[array_name]
        
        # Process each column once and assign to appropriate collection
        for col_name, column in model.columns.items():
            if cls._should_exclude_from_all_views(column, hierarchy):
                excluded_columns.add(col_name)
                continue
                
            # Check if this column belongs to a nested view
            array_parent = cls._find_array_parent(col_name, array_model_names)
            
            if array_parent:
                # This column belongs to a nested view
                if array_parent not in nested_view_columns:
                    nested_view_columns[array_parent] = {}
                nested_view_columns[array_parent][col_name] = column
            else:
                # This column belongs to the main view
                main_view_columns[col_name] = column
        
        # Add intermediate STRUCT columns to nested views (like supplierinformation.gtin)
        for array_name in array_model_names:
            if array_name in nested_view_columns:
                # Find all intermediate STRUCT paths for this array
                for col_name in model.columns.keys():
                    if col_name.startswith(f"{array_name}.") and col_name not in nested_view_columns[array_name]:
                        # Check if this is an intermediate STRUCT (has children)
                        has_children = any(
                            other_name.startswith(f"{col_name}.")
                            for other_name in model.columns.keys()
                        )
                        if has_children and col_name in model.columns:
                            nested_view_columns[array_name][col_name] = model.columns[col_name]
        
        return cls(
            main_view_columns=main_view_columns,
            nested_view_columns=nested_view_columns,
            excluded_columns=excluded_columns
        )
    
    @staticmethod
    def _build_hierarchy_map(columns: Dict[str, DbtModelColumn]) -> Dict[str, Dict]:
        """Build a map of parent -> children relationships based on dot notation."""
        hierarchy = {}
        for col in columns.values():
            parts = col.name.split('.')
            for i in range(len(parts)):
                parent_path = '.'.join(parts[:i+1])
                if parent_path not in hierarchy:
                    hierarchy[parent_path] = {
                        'children': set(),
                        'is_array': col.data_type and 'ARRAY' in str(col.data_type).upper() if i == len(parts) - 1 else False,
                        'column': col if i == len(parts) - 1 else None
                    }
                
                # Add child relationships
                if i < len(parts) - 1:
                    child_path = '.'.join(parts[:i+2])
                    hierarchy[parent_path]['children'].add(child_path)
        return hierarchy
    
    @staticmethod
    def _should_exclude_from_all_views(column: DbtModelColumn, hierarchy: Dict) -> bool:
        """Check if a column should be excluded from all views."""
        # Exclude STRUCT parents that have children (but not ARRAY<STRUCT>)
        if column.data_type and "STRUCT" in str(column.data_type).upper():
            data_type_str = str(column.data_type).upper()
            if not data_type_str.startswith('ARRAY'):
                # Check if this STRUCT has nested children
                return any(
                    other_path.startswith(f"{column.name}.")
                    for other_path in hierarchy.keys()
                )
        return False
    
    @staticmethod
    def _find_array_parent(col_name: str, array_model_names: Set[str]) -> str:
        """Find which array model this column belongs to, if any."""
        # Find the most specific (longest) array parent that matches
        matching_arrays = []
        for array_name in array_model_names:
            if col_name.startswith(f"{array_name}."):
                matching_arrays.append(array_name)
        
        # Return the longest match (most specific)
        if matching_arrays:
            return max(matching_arrays, key=len)
        return None
