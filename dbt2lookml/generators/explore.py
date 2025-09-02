"""LookML explore generator module."""

import logging
from typing import Any

from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


class LookmlExploreGenerator:
    """Lookml explore generator."""

    def __init__(self, args):
        self._cli_args = args

    def _group_strings(
        self, all_columns: list[DbtModelColumn], array_columns: list[DbtModelColumn]
    ) -> dict:
        """Group strings into a nested structure."""
        nested_columns = {}

        def remove_parts(input_string):
            parts = input_string.split('.')
            modified_parts = parts[:-1]
            result = '.'.join(modified_parts)
            return result

        def recurse(parent: DbtModelColumn, all_columns: list[DbtModelColumn], level=0):
            structure = {'column': parent, 'children': []}
            for column in all_columns:
                if column.data_type in ('ARRAY', 'STRUCT'):
                    # If ARRAY<INT64> or likeworthy
                    if len(column.inner_types) == 1 and ' ' not in column.inner_types[0]:
                        structure['children'].append(
                            {column.name: {'column': column, 'children': []}}
                        )
                    # Normal ARRAY or STRUCT
                    else:
                        structure['children'].append(
                            {
                                column.name: recurse(
                                    parent=column,
                                    all_columns=[
                                        d
                                        for d in all_columns
                                        if remove_parts(d.name) == column.name
                                    ],
                                    level=level + 1,
                                )
                            }
                        )
                else:
                    structure['children'].append({column.name: {'column': column, 'children': []}})
            return structure

        for parent in array_columns:
            nested_columns[parent.name] = recurse(
                parent, [d for d in all_columns if remove_parts(d.name) == parent.name]
            )
        return nested_columns

    def recurse_joins(self, structure: dict, model: DbtModel) -> list[dict[str, Any]]:
        """Recursively build joins for nested structures."""
        if not structure:
            return []

        # Import centralized camel_to_snake function
        from dbt2lookml.utils import camel_to_snake

        join_list: list[dict[str, Any]] = []
        # Sort array models by nesting depth to ensure parent views are created first
        sorted_array_models = sorted(structure.items(), key=lambda x: x[0].count('.'))
        
        for parent, children in sorted_array_models:
            # Use table name from relation_name if use_table_name is True
            # Apply same transformation as view names
            if self._cli_args.use_table_name:
                table_name = model.relation_name.split('.')[-1].strip('`')
                base_name = camel_to_snake(table_name)
            else:
                base_name = model.name.lower()

            # Find the original column to get the CamelCase name
            parent_column = model.columns.get(parent)
            if parent_column and parent_column.original_name:
                original_parent_name = parent_column.original_name
            else:
                original_parent_name = parent

            # Convert parent to snake_case using original CamelCase name
            snake_parent = '__'.join([camel_to_snake(part) for part in original_parent_name.split('.')])
            view_name = f"{base_name}__{snake_parent}"

            # Use the same snake_case conversion for dimension name using original name
            dimension_name = camel_to_snake(original_parent_name.split('.')[-1])

            # Create SQL join for array unnesting
            # Pattern: UNNEST(${[parent].[dimension]}) AS [parent]__[view_name]
            
            # Check if this array has a parent ARRAY view that already exists as a join
            # This determines if we reference the parent view or the base model
            has_parent_view = False
            parent_view_name = None
            if '.' in original_parent_name:
                # Find the closest parent ARRAY view by checking progressively shorter paths
                parent_parts = original_parent_name.split('.')[:-1]
                for i in range(len(parent_parts), 0, -1):
                    candidate_parts = parent_parts[:i]
                    candidate_view_name = f"{base_name}__{'__'.join([camel_to_snake(part) for part in candidate_parts])}"
                    # Check if this candidate parent view exists in our join list
                    if any(join['name'] == candidate_view_name for join in join_list):
                        has_parent_view = True
                        parent_view_name = candidate_view_name
                        break
            
            if has_parent_view:
                # This is a nested array - reference the parent view
                # Calculate the dimension path from parent view to this field
                parent_depth = len(parent_view_name.split('__')) - 1  # Subtract 1 for base_name
                current_parts = original_parent_name.split('.')
                dimension_path_parts = current_parts[parent_depth:]
                dimension_path = '__'.join([camel_to_snake(part) for part in dimension_path_parts])
                join_sql = f'LEFT JOIN UNNEST(${{{parent_view_name}.{dimension_path}}}) AS {view_name}'
            else:
                # This is a top-level array (including flattened) - reference the base model
                flattened_field_name = snake_parent
                join_sql = f'LEFT JOIN UNNEST(${{{base_name}.{flattened_field_name}}}) AS {view_name}'
            
            # Set required_joins for nested arrays
            join_dict = {
                'name': view_name,
                'relationship': 'one_to_many',
                'sql': join_sql,
                'type': 'left_outer',
            }
            
            # Note: required_joins are not included to match fixture expectations
            # The fixtures expect joins without required_joins field
            
            join_list.append(join_dict)
            # Process nested arrays within this array
            for child_structure in children['children']:
                for child_name, child_dict in child_structure.items():
                    if len(child_dict['children']) > 0:
                        child_view_name = f"{base_name}__{child_name.replace('.', '__')}"
                        join_name = f"${{{view_name}.{child_name.split('.')[-1]}}}"
                        join_sql = f'LEFT JOIN UNNEST({join_name}) AS {child_view_name}'
                        join_list.append(
                            {
                                'name': child_view_name,
                                'relationship': 'one_to_many',
                                'sql': join_sql,
                                'type': 'left_outer',
                            }
                        )
                        # Recursively process any deeper nested arrays
                        join_list.extend(self.recurse_joins(child_structure, model))
        return join_list

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
