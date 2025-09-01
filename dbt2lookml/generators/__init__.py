"""LookML Generator implementations."""

import os
from typing import Dict, Tuple

from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.generators.explore import LookmlExploreGenerator
from dbt2lookml.generators.measure import LookmlMeasureGenerator
from dbt2lookml.generators.view import LookmlViewGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


class LookmlGenerator:
    """Main LookML generator that coordinates dimension, view, and explore generation."""

    def __init__(self, cli_args):
        self._cli_args = cli_args
        self.dimension_generator = LookmlDimensionGenerator(cli_args)
        self.view_generator = LookmlViewGenerator(cli_args)
        self.explore_generator = LookmlExploreGenerator(cli_args)
        self.measure_generator = LookmlMeasureGenerator(cli_args)

    def _get_view_label(self, model: DbtModel) -> str:
        """Get the view label from the model metadata or name."""
        # Check looker meta view label first
        if (hasattr(model.meta, 'looker') and model.meta.looker is not None and
            hasattr(model.meta.looker, 'view') and model.meta.looker.view is not None and
            hasattr(model.meta.looker.view, 'label') and model.meta.looker.view.label is not None):
            return model.meta.looker.view.label
        # Fall back to model name if available
        return model.name.replace("_", " ").title() if hasattr(model, 'name') else None

    def _extract_array_models(self, columns: list[DbtModelColumn]) -> list[DbtModelColumn]:
        """Extract array models from a list of columns.
        
        Only extracts ARRAY<STRUCT> types that need nested views.
        ARRAY<PRIMITIVE> types (like ARRAY<STRING>) should not create nested views.
        """
        array_models = []
        for column in columns:
            if column.data_type is not None:
                data_type_str = str(column.data_type).upper()
                # Must start with ARRAY to be an array type (not just contain ARRAY)
                if data_type_str.startswith('ARRAY'):
                    # Check if it's ARRAY<STRUCT> by looking at inner_types or checking for nested columns
                    if (column.inner_types and len(column.inner_types) > 0) or \
                       any(other_col.name.startswith(f"{column.name}.") for other_col in columns):
                        array_models.append(column)
        return array_models

    def _get_excluded_array_names(self, model: DbtModel, array_models: list) -> list:
        """Get list of dimension names to exclude from main view."""
        exclude_names = []
        
        # Exclude children of array models (they belong in nested views)
        # But DO NOT exclude the array parent dimensions - they should be present but hidden
        for array_model in array_models:
            exclude_names.extend(
                col.name.replace('.', '__')  # Convert to dimension name format
                for col in model.columns.values()
                if col.name.startswith(f"{array_model.name}.")
            )

        # Exclude parent STRUCT fields that have children (but not ARRAY<STRUCT> parents)
        # ARRAY<STRUCT> parents should be present as hidden dimensions in main view
        for col in model.columns.values():
            if col.data_type and "STRUCT" in str(col.data_type).upper():
                # Skip if this is an ARRAY<STRUCT> - those should be present but hidden
                data_type_str = str(col.data_type).upper()
                if data_type_str.startswith('ARRAY'):
                    continue
                    
                # Check if this STRUCT has nested children
                has_children = any(
                    other_col.name.startswith(f"{col.name}.")
                    for other_col in model.columns.values()
                )
                if has_children:
                    exclude_names.append(col.name.replace('.', '__'))  # Convert to dimension name format

        return exclude_names

    def _get_unique_view_name(self, model: DbtModel) -> str:
        """Extract unique view name from model unique_id to handle versioned models.
        
        Args:
            model: The dbt model
        Returns:
            str: Unique view name including version if present
        Example:
            model.bica.my_model.v1 -> my_model_v1
            model.bica.my_model -> my_model
        """
        # Extract the last part of unique_id which contains name and optional version
        unique_id_parts = model.unique_id.split('.')
        if len(unique_id_parts) >= 3:
            # For versioned models: model.package.name.version
            # For non-versioned: model.package.name
            if len(unique_id_parts) > 3:
                # Versioned model: get name and version
                model_part = unique_id_parts[-2]  # The name part
                version_part = unique_id_parts[-1]  # The version part
                if version_part.startswith('v'):
                    return f"{model_part}_{version_part}".lower()
                else:
                    return model_part.lower()
            else:
                # Non-versioned model: get the last part (name)
                return unique_id_parts[-1].lower()
        return model.name.lower()

    def _get_file_path(self, model: DbtModel, view_name: str) -> str:
        """Get the file path for the LookML view.
        Args:
            model: The dbt model to generate a view for
            view_name: The name to use for the view
        Returns:
            str: The full file path for the LookML view file
        Example:
            >>> model = DbtModel(name="my_model", path="/path/to/models/my_model.sql")
            >>> generator._get_file_path(model, "my_view")
            "/path/to/models/my_view.view.lkml"
        """
        if self._cli_args.use_table_name:
            # When using table names, use the directory structure from the model path
            # but don't include the model name in the path
            directory = os.path.dirname(model.path)
            # Use relation_name but preserve version info for uniqueness
            file_name = model.relation_name.split('.')[-1].strip('`').lower()
            return os.path.join(directory, f'{file_name}.view.lkml')
        else:
            # Original behavior for model names - use unique view name to avoid collisions
            file_path = os.path.join(model.path.split(model.name)[0])
            unique_view_name = self._get_unique_view_name(model)
            return f'{file_path}/{unique_view_name}.view.lkml'

    def generate(self, model: DbtModel) -> Tuple[str, Dict]:
        """Generate LookML for a model.
        Args:
            model: The dbt model to generate LookML for
        Returns:
            tuple[str, dict]: A tuple containing:
                - str: The file path where the LookML should be written
                - dict: The generated LookML content
        Raises:
            ValueError: If the model is missing required attributes
        """
        # Get view name - use unique name to handle versioned models
        view_name = (
            model.relation_name.split('.')[-1].strip('`').lower()
            if self._cli_args.use_table_name
            else self._get_unique_view_name(model)
        )
        # Get view label - use the helper method to get proper label
        view_label = self._get_view_label(model)
        # Get array models and structure
        array_models = self._extract_array_models(list(model.columns.values()))
        exclude_names = self._get_excluded_array_names(model, array_models)
        # Create main view
        views = self.view_generator.generate(
            model=model,
            view_name=view_name,
            view_label=view_label,
            exclude_names=exclude_names,
            array_models=array_models,
            dimension_generator=self.dimension_generator,
            measure_generator=self.measure_generator,
        )
        # Create LookML base
        lookml = {
            'view': views,
        }
        # Always create explore to ensure SQL references are valid
        explore = self.explore_generator.generate(
            model=model,
            view_name=view_name,
            view_label=view_label,
            array_models=array_models,
        )
        lookml['explore'] = explore
        return self._get_file_path(model, view_name), lookml


__all__ = ['LookmlGenerator']
