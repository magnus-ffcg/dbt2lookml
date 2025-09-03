"""Column collections for organizing model columns by their intended use."""

from dataclasses import dataclass
from typing import Dict, List, Set

from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


@dataclass
class ColumnCollections:
    """Pre-structured column collections to avoid filtering during generation."""

    main_view_columns: Dict[str, DbtModelColumn]
    nested_view_columns: Dict[str, Dict[str, DbtModelColumn]]  # array_name -> columns
    excluded_columns: Dict[str, DbtModelColumn]  # For reference/debugging

    @classmethod
    def from_model(cls, model: DbtModel, array_models: List[str] = None) -> 'ColumnCollections':
        """Create column collections from a dbt model with optimized processing."""
        if array_models is None:
            array_models = []

        # Get all columns from the model
        all_columns = model.columns

        # Build hierarchy map for proper nested array detection
        hierarchy = cls._build_hierarchy_map(all_columns)

        # Convert array_models to string names if they're DbtModelColumn objects
        if array_models and len(array_models) > 0 and hasattr(array_models[0], 'name'):
            array_model_names = set(col.name for col in array_models)
        else:
            array_model_names = set(array_models)

        # Find all array columns (including nested ones) from hierarchy
        for col_name, col_info in hierarchy.items():
            if col_info['is_array'] and col_info['column']:
                array_model_names.add(col_name)

        # Single-pass column classification with proper nested array handling
        main_view_columns = {}
        nested_view_columns = {}
        excluded_columns = {}

        for col_name, column in all_columns.items():
            # Check if column should be excluded from all views
            if cls._should_exclude_from_all_views(column, hierarchy):
                excluded_columns[col_name] = column
                continue

            # Find the most specific array parent
            array_parent = cls._find_array_parent(col_name, array_model_names)

            # Array parent columns need special handling
            if col_name in array_model_names:
                # Check if this array has child columns
                has_children = any(other_name.startswith(f"{col_name}.") for other_name in all_columns.keys())

                # Check if this array is itself a child of another array (not just any parent)
                is_nested_array = array_parent is not None and array_parent in array_model_names

                if has_children:
                    # Array with children (ARRAY<STRUCT>): add to main view only if not nested under another array, always create nested view
                    if not is_nested_array:
                        main_view_columns[col_name] = column
                    if col_name not in nested_view_columns:
                        nested_view_columns[col_name] = {}
                    # For ARRAY<STRUCT> fields, add the array field itself to its nested view as a hidden dimension
                    nested_view_columns[col_name][col_name] = column
                else:
                    # Array without children (pure ARRAY): add to main view only if not nested under another array, always create nested view
                    if not is_nested_array:
                        main_view_columns[col_name] = column
                    if col_name not in nested_view_columns:
                        nested_view_columns[col_name] = {}
                    # For pure ARRAY fields, add the array field itself to its nested view
                    nested_view_columns[col_name][col_name] = column

                # If this array is nested under another array, also add it to the parent's nested view
                if is_nested_array and array_parent:
                    if array_parent not in nested_view_columns:
                        nested_view_columns[array_parent] = {}
                    nested_view_columns[array_parent][col_name] = column
            elif array_parent:
                # This column belongs to a nested view
                if array_parent not in nested_view_columns:
                    nested_view_columns[array_parent] = {}
                nested_view_columns[array_parent][col_name] = column
            else:
                # This column belongs to the main view
                main_view_columns[col_name] = column

        return cls(
            main_view_columns=main_view_columns, nested_view_columns=nested_view_columns, excluded_columns=excluded_columns
        )

    @staticmethod
    def _build_hierarchy_map(columns: Dict[str, DbtModelColumn]) -> Dict[str, Dict]:
        """Build a map of parent -> children relationships based on dot notation."""
        hierarchy = {}

        # First pass: create all hierarchy entries
        for col in columns.values():
            parts = col.name.split('.')
            for i in range(len(parts)):
                parent_path = '.'.join(parts[: i + 1])
                if parent_path not in hierarchy:
                    hierarchy[parent_path] = {'children': set(), 'is_array': False, 'column': None}

        # Second pass: set correct column references and array flags
        for col in columns.values():
            if col.name in hierarchy:
                hierarchy[col.name]['column'] = col
                # Only mark as array if the data type starts with ARRAY (not just contains it)
                hierarchy[col.name]['is_array'] = col.data_type and str(col.data_type).upper().startswith('ARRAY')

        # Third pass: build child relationships
        for col in columns.values():
            parts = col.name.split('.')
            for i in range(len(parts) - 1):
                parent_path = '.'.join(parts[: i + 1])
                child_path = '.'.join(parts[: i + 2])
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
                return any(other_path.startswith(f"{column.name}.") for other_path in hierarchy.keys())
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
