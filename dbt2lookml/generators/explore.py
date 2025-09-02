"""LookML explore generator module."""
import logging
from typing import Any, Dict, List, Optional

from dbt2lookml.models.dbt import DbtModel, DbtModelColumn
from dbt2lookml.utils import camel_to_snake


class LookmlExploreGenerator:
    """Lookml explore generator."""

    def __init__(self, args):
        self._cli_args = args

    def _group_strings(
        self, all_columns: list[DbtModelColumn], array_columns: list[DbtModelColumn]
    ) -> dict:
        """Group strings into a nested structure."""
        nested_columns = {}

        for parent in array_columns:
            child_columns = self._filter_child_columns(all_columns, parent.name)
            nested_columns[parent.name] = self._build_nested_structure(parent, child_columns)
            
        return nested_columns

    def _filter_child_columns(self, all_columns: list[DbtModelColumn], parent_name: str) -> list[DbtModelColumn]:
        """Filter columns to find children of a specific parent.
        
        Args:
            all_columns: All available columns
            parent_name: Name of the parent column
            
        Returns:
            List of child columns that belong to the parent
        """
        return [
            column for column in all_columns 
            if self._remove_last_part(column.name) == parent_name
        ]
    
    def _remove_last_part(self, input_string: str) -> str:
        """Remove the last part of a dot-separated string.
        
        Args:
            input_string: Dot-separated string (e.g., 'items.details.name')
            
        Returns:
            String with last part removed (e.g., 'items.details')
        """
        parts = input_string.split('.')
        modified_parts = parts[:-1]
        return '.'.join(modified_parts)
    
    def _build_nested_structure(self, parent: DbtModelColumn, child_columns: list[DbtModelColumn], level: int = 0) -> dict:
        """Build nested structure for a parent column and its children.
        
        Args:
            parent: Parent column to build structure for
            child_columns: Child columns belonging to the parent
            level: Current nesting level (for recursion tracking)
            
        Returns:
            Nested structure dictionary with column and children
        """
        structure = {'column': parent, 'children': []}
        
        for column in child_columns:
            if column.data_type in ('ARRAY', 'STRUCT'):
                child_structure = self._create_child_structure(column, child_columns, level)
                structure['children'].append({column.name: child_structure})
            else:
                # Simple leaf column
                structure['children'].append({
                    column.name: {'column': column, 'children': []}
                })
                
        return structure
    
    def _create_child_structure(self, column: DbtModelColumn, all_child_columns: list[DbtModelColumn], level: int) -> dict:
        """Create structure for a child ARRAY or STRUCT column.
        
        Args:
            column: The ARRAY or STRUCT column
            all_child_columns: All child columns to search within
            level: Current nesting level
            
        Returns:
            Structure dictionary for the child column
        """
        # Check if this is a simple array with single inner type
        if self._is_simple_array(column):
            return {'column': column, 'children': []}
        
        # For complex arrays/structs, recurse to build nested structure
        nested_child_columns = self._filter_child_columns(all_child_columns, column.name)
        return self._build_nested_structure(column, nested_child_columns, level + 1)
    
    def _is_simple_array(self, column: DbtModelColumn) -> bool:
        """Check if column is a simple array with single inner type and no spaces.
        
        Args:
            column: Column to check
            
        Returns:
            True if it's a simple array (e.g., ARRAY<INT64>), False otherwise
        """
        return (
            column.data_type == 'ARRAY' and 
            hasattr(column, 'inner_types') and 
            column.inner_types and
            len(column.inner_types) == 1 and 
            ' ' not in column.inner_types[0]
        )

    def recurse_joins(self, structure: dict, model: DbtModel) -> list[dict[str, Any]]:
        """Recursively build joins for nested structures."""
        if not structure:
            return []

        join_list: list[dict[str, Any]] = []
        
        # Sort array models by nesting depth to ensure parent views are created first
        sorted_array_models = sorted(structure.items(), key=lambda x: x[0].count('.'))
        
        for parent, children in sorted_array_models:
            # Create join for this array model
            join_dict = self._create_array_join(parent, model, join_list)
            join_list.append(join_dict)
            
            # Process nested arrays within this array
            nested_joins = self._process_nested_children(children, model, join_dict['name'])
            join_list.extend(nested_joins)
            
        return join_list

    def _create_array_join(self, parent: str, model: DbtModel, existing_joins: list[dict[str, Any]]) -> dict[str, Any]:
        """Create a join dictionary for an array model.
        
        Args:
            parent: Parent array field name
            model: DBT model containing the array
            existing_joins: List of existing joins for parent view detection
            
        Returns:
            Join dictionary for the array
        """
        
        # Determine base name
        base_name = self._get_base_name(model)
        
        # Get original name for CamelCase conversion
        original_parent_name = self._get_original_name(parent, model)
        
        # Create view name
        snake_parent = '__'.join([camel_to_snake(part) for part in original_parent_name.split('.')])
        view_name = f"{base_name}__{snake_parent}"
        
        # Generate join SQL
        join_sql = self._generate_join_sql(original_parent_name, base_name, view_name, existing_joins)
        
        return {
            'name': view_name,
            'relationship': 'one_to_many',
            'sql': join_sql,
            'type': 'left_outer',
        }
    
    def _get_base_name(self, model: DbtModel) -> str:
        """Get the base name for joins based on CLI args.
        
        Args:
            model: DBT model
            
        Returns:
            Base name for join references
        """
        
        if self._cli_args.use_table_name:
            table_name = model.relation_name.split('.')[-1].strip('`')
            return camel_to_snake(table_name)
        else:
            return model.name.lower()
    
    def _get_original_name(self, parent: str, model: DbtModel) -> str:
        """Get the original CamelCase name for a parent field.
        
        Args:
            parent: Parent field name
            model: DBT model containing the field
            
        Returns:
            Original name or fallback to parent name
        """
        parent_column = model.columns.get(parent)
        if parent_column and parent_column.original_name:
            return parent_column.original_name
        return parent
    
    def _generate_join_sql(self, original_parent_name: str, base_name: str, view_name: str, existing_joins: list[dict[str, Any]]) -> str:
        """Generate the SQL for joining an array.
        
        Args:
            original_parent_name: Original CamelCase parent name
            base_name: Base model name
            view_name: Target view name for the join
            existing_joins: List of existing joins for parent detection
            
        Returns:
            SQL string for the join
        """
        
        # Check for parent view
        parent_view_info = self._find_parent_view(original_parent_name, base_name, existing_joins)
        
        if parent_view_info:
            # Reference parent view
            parent_view_name, dimension_path = parent_view_info
            return f'LEFT JOIN UNNEST(${{{parent_view_name}.{dimension_path}}}) AS {view_name}'
        else:
            # Reference base model
            snake_parent = '__'.join([camel_to_snake(part) for part in original_parent_name.split('.')])
            return f'LEFT JOIN UNNEST(${{{base_name}.{snake_parent}}}) AS {view_name}'
    
    def _find_parent_view(self, original_parent_name: str, base_name: str, existing_joins: list[dict[str, Any]]) -> Optional[tuple[str, str]]:
        """Find parent view for nested array references.
        
        Args:
            original_parent_name: Original CamelCase parent name
            base_name: Base model name
            existing_joins: List of existing joins
            
        Returns:
            Tuple of (parent_view_name, dimension_path) or None if no parent
        """
        
        if '.' not in original_parent_name:
            return None
            
        # Find the closest parent ARRAY view by checking progressively shorter paths
        parent_parts = original_parent_name.split('.')[:-1]
        for i in range(len(parent_parts), 0, -1):
            candidate_parts = parent_parts[:i]
            candidate_view_name = f"{base_name}__{'__'.join([camel_to_snake(part) for part in candidate_parts])}"
            
            # Check if this candidate parent view exists in existing joins
            if any(join['name'] == candidate_view_name for join in existing_joins):
                # Calculate dimension path from parent view to this field
                parent_depth = len(candidate_view_name.split('__')) - 1  # Subtract 1 for base_name
                current_parts = original_parent_name.split('.')
                dimension_path_parts = current_parts[parent_depth:]
                dimension_path = '__'.join([camel_to_snake(part) for part in dimension_path_parts])
                return candidate_view_name, dimension_path
                
        return None
    
    def _process_nested_children(self, children: dict, model: DbtModel, parent_view_name: str) -> list[dict[str, Any]]:
        """Process nested children arrays within a parent array.
        
        Args:
            children: Children structure from parent array
            model: DBT model
            parent_view_name: Name of the parent view
            
        Returns:
            List of join dictionaries for nested children
        """
        nested_joins = []
        base_name = self._get_base_name(model)
        
        for child_structure in children['children']:
            for child_name, child_dict in child_structure.items():
                if len(child_dict['children']) > 0:
                    child_view_name = f"{base_name}__{child_name.replace('.', '__')}"
                    join_name = f"${{{parent_view_name}.{child_name.split('.')[-1]}}}"
                    join_sql = f'LEFT JOIN UNNEST({join_name}) AS {child_view_name}'
                    
                    nested_joins.append({
                        'name': child_view_name,
                        'relationship': 'one_to_many',
                        'sql': join_sql,
                        'type': 'left_outer',
                    })
                    
                    # Recursively process any deeper nested arrays
                    nested_joins.extend(self.recurse_joins(child_structure, model))
                    
        return nested_joins

    def generate(
        self, model: DbtModel, view_name: str, view_label: str, array_models: list[DbtModelColumn]
    ) -> dict[str, Any]:
        """Create the explore definition."""
        # Get nested structure for joins
        structure = self._group_strings(list(model.columns.values()), array_models)
        # Create explore
        explore: dict[str, Any] = {
            'name': view_name,
            'label': view_label,
            'from': view_name,
            'hidden': 'no',
        }
        # Add joins if present
        if joins := self.recurse_joins(structure, model):
            logging.debug(f"Adding {len(joins)} joins to explore")
            explore['joins'] = joins
        return explore
