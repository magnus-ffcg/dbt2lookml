"""LookML explore generator module."""

import logging

from typing import Dict, List, Optional
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

    def recurse_joins(self, structure, model):
        """Recursively build joins for nested structures."""
        if not structure:
            return []

        join_list = []
        for parent, children in structure.items():
            # Use table name from relation_name if use_table_name is True
            base_name = (
                model.relation_name.split('.')[-1].strip('`')
                if self._cli_args.use_table_name
                else model.name
            )
            view_name = f"{base_name}__{parent.replace('.','__')}"

            # Create SQL join for array unnesting
            join_sql = f'LEFT JOIN UNNEST(${{{base_name}.{parent}}}) AS {view_name}'

            # Add to list
            join_list.append(
                {
                    'name': view_name,
                    'relationship': 'one_to_many',
                    'sql': join_sql,
                    'type': 'left_outer',
                    'required_joins': [],  # No required joins for top-level arrays
                }
            )

            # Process nested arrays within this array
            for child_structure in children['children']:
                for child_name, child_dict in child_structure.items():
                    if len(child_dict['children']) > 0:
                        child_view_name = f"{base_name}__{child_name.replace('.','__')}"
                        child_join_sql = f'LEFT JOIN UNNEST(${{{view_name}.{child_name.split(".")[-1]}}}) AS {child_view_name}'

                        join_list.append(
                            {
                                'name': child_view_name,
                                'relationship': 'one_to_many',
                                'sql': child_join_sql,
                                'type': 'left_outer',
                                'required_joins': [view_name],  # This join requires the parent view
                            }
                        )

                        # Recursively process any deeper nested arrays
                        join_list.extend(self.recurse_joins(child_structure, model))

        return join_list

    def generate(
        self, model: DbtModel, view_name: str, view_label: str, array_models: list
    ) -> dict:
        """Create the explore definition."""
        # Get nested structure for joins
        structure = self._group_strings(list(model.columns.values()), array_models)

        # Check if model.meta.looker exists and has hidden attribute
        hidden = 'no'
        if (
            hasattr(model, 'meta')
            and hasattr(model.meta, 'looker')
            and hasattr(model.meta.looker, 'hidden')
        ):
            hidden = 'yes' if model.meta.looker.hidden else 'no'

        # Create explore
        explore = {
            'name': view_name,
            'label': view_label,
            'from': view_name,
            'hidden': hidden,
        }

        # Add joins if present
        if joins := self.recurse_joins(structure, model):
            logging.info(f"Adding {len(joins)} joins to explore")
            explore['joins'] = joins

        return explore
