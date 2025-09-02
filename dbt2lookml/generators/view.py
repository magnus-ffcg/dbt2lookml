"""LookML view generator module."""
import logging
from typing import Any, Dict

from dbt2lookml.models.column_collections import ColumnCollections
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


class LookmlViewGenerator:
    """Lookml view generator."""

    def __init__(self, args):
        """Initialize the generator with CLI arguments."""
        self._cli_args = args
        self._column_collections_cache = {}  # Cache column collections per model

    def _create_main_view(
        self,
        model: DbtModel,
        view_name: str,
        view_label: str,
        exclude_names: list,
        dimension_generator,
        measure_generator,
        array_models: list = None,
    ) -> dict:
        """Create the main view definition."""
        # Build view dict in specific order to match expected LookML output
        view = {
            'name': view_name.lower(),
            'sql_table_name': model.relation_name,
        }
        
        # Use provided array models or empty list if none provided
        if array_models is None:
            array_models = []
        
        # Cache column collections to avoid repeated expensive processing
        # Create hashable cache key from array model names
        array_model_names = [getattr(am, 'name', str(am)) for am in array_models]
        cache_key = (model.unique_id, tuple(sorted(array_model_names)))
        if cache_key not in self._column_collections_cache:
            self._column_collections_cache[cache_key] = ColumnCollections.from_model(model, array_models)
        collections = self._column_collections_cache[cache_key]
        
        dimensions, nested_dimensions = dimension_generator.lookml_dimensions_from_model(
            model, columns_subset=collections.main_view_columns
        )
        
        # Add nested array dimensions to main view
        if nested_dimensions:
            if dimensions is None:
                dimensions = []
            dimensions.extend(nested_dimensions)

        # Get dimension groups
        dimension_groups_result = dimension_generator.lookml_dimension_groups_from_model(
            model, columns_subset=collections.main_view_columns
        )
        conflicting_dimensons = []
        dimension_groups = dimension_groups_result.get('dimension_groups', [])
        # Comment out conflicting regular dimensions instead of timeframes
        if dimensions and dimension_groups:
            dimensions, conflicting_dimensons = dimension_generator._comment_conflicting_dimensions(
                dimensions, dimension_groups, model.name
            )
        # Clean dimension groups for output (remove internal fields)
        if dimension_groups:
            dimension_groups = dimension_generator._clean_dimension_groups_for_output(
                dimension_groups
            )
        # Add dimensions and dimension_groups to view
        if dimensions:
            view['dimensions'] = dimensions
        if dimension_groups:
            view['dimension_groups'] = dimension_groups
        # Generate measures from metadata
        measures = measure_generator.lookml_measures_from_model(
            model, columns_subset=collections.main_view_columns
        )
        
        # Add default count measure if no measures exist
        if not measures:
            measures = [{'name': 'count', 'type': 'count'}]
        
        view['measures'] = measures
        if hidden := model._get_meta_looker('view', 'hidden'):
            view['hidden'] = 'yes' if hidden else 'no'

        if len(conflicting_dimensons) > 0:
            view['# Removed conflicting dimensions: ' + ','.join(conflicting_dimensons)] = ""
            
        
        return view

    def _is_yes_no(self, model: DbtModel) -> str:
        """Check if model should be hidden."""
        hidden = 'no'
        if (
            model.meta is not None
            and model.meta.looker is not None
            and hasattr(model.meta.looker, 'view')
            and model.meta.looker.view is not None
            and hasattr(model.meta.looker.view, 'hidden')
        ):
            hidden = 'yes' if model.meta.looker.view.hidden else 'no'
        return hidden

    def _create_nested_view(
        self,
        model: DbtModel,
        base_name: str,
        array_model: DbtModelColumn,
        view_label: str,
        dimension_generator,
        measure_generator,
        array_models: list = None,
    ) -> dict[str, Any]:
        """Create a nested view definition for an array field."""
        # Generate nested view name
        if self._cli_args.use_table_name:
            from dbt2lookml.utils import camel_to_snake
            array_model_name = array_model.lookml_long_name
            table_name = model.relation_name.split('.')[-1].strip('`')
            base_name = camel_to_snake(table_name)
            nested_view_name = f"{base_name}__{array_model_name}"
        else:
            nested_view_name = f"{base_name}__{array_model.lookml_long_name}".lower()
        
        # Get column collections for this nested view
        if array_models is None:
            array_models = []
        
        # Cache column collections to avoid repeated expensive processing
        array_model_names = [getattr(am, 'name', str(am)) for am in array_models]
        cache_key = (model.unique_id, tuple(sorted(array_model_names)))
        if cache_key not in self._column_collections_cache:
            self._column_collections_cache[cache_key] = ColumnCollections.from_model(model, array_models)
        collections = self._column_collections_cache[cache_key]
        
        # Get columns for this specific nested view
        nested_columns = collections.nested_view_columns.get(array_model.name, {})
        
        # Generate dimensions from the dimension generator
        # Let the dimension generator handle all dimension creation logic
        dimensions, nested_dimensions = dimension_generator.lookml_dimensions_from_model(
            model, columns_subset=nested_columns, is_nested_view=True, array_model_name=array_model.name
        )
        
        # Get dimension groups
        dimension_groups_result = dimension_generator.lookml_dimension_groups_from_model(
            model, columns_subset=nested_columns, is_nested_view=True, array_model_name=array_model.name
        )
        dimension_groups = dimension_groups_result.get('dimension_groups', [])
        
        # Apply conflict detection
        conflicting_dimensions = []
        if dimensions and dimension_groups:
            dimensions, conflicting_dimensions = dimension_generator._comment_conflicting_dimensions(
                dimensions, dimension_groups, model.name
            )
        
        # Clean dimension groups for output
        if dimension_groups:
            dimension_groups = dimension_generator._clean_dimension_groups_for_output(dimension_groups)
        
        # Only create nested view if it has content
        if not dimensions and not dimension_groups:
            measures = measure_generator.lookml_measures_from_model(
                model, columns_subset=nested_columns
            )
            if not measures:
                return None  # Don't create empty nested views
        
        nested_view = {'name': nested_view_name}
        if dimensions:
            nested_view['dimensions'] = dimensions
        if dimension_groups:
            nested_view['dimension_groups'] = dimension_groups
        if measures := measure_generator.lookml_measures_from_model(
            model, columns_subset=nested_columns
        ):
            nested_view['measures'] = measures
            
        # Add dimension removal comment for nested views too
        if len(conflicting_dimensions) > 0:
            nested_view['# Removed conflicting dimensions: ' + ','.join(conflicting_dimensions)] = ""
            
        return nested_view

    def generate(
        self,
        model: DbtModel,
        view_name: str,
        view_label: str,
        exclude_names: list,
        array_models: list,
        dimension_generator,
        measure_generator,
    ) -> Dict:
        """Generate a view for a model."""
        main_view = self._create_main_view(
            model, view_name, view_label, exclude_names, dimension_generator, measure_generator, array_models
        )
        views = [main_view]
        
        # Create nested views recursively for all array models
        self._create_nested_views_recursive(
            views, model, view_name, view_label, array_models, dimension_generator, measure_generator
        )
        return views
    
    def _create_nested_views_recursive(
        self,
        views: list,
        model: DbtModel,
        view_name: str,
        view_label: str,
        array_models: list,
        dimension_generator,
        measure_generator,
    ):
        """Recursively create nested views for all array models."""
        # Extract all array models from the model using the same logic as the main generator
        from dbt2lookml.models.column_collections import ColumnCollections
        
        # Use hierarchy-based logic to find ALL arrays (including deeply nested ones)
        columns_dict = {col.name: col for col in model.columns.values()}
        hierarchy = ColumnCollections._build_hierarchy_map(columns_dict)
        
        all_array_models = []
        for col_name, col_info in hierarchy.items():
            if col_info['is_array'] and col_info['column']:
                all_array_models.append(col_info['column'])
        
        # Create nested views for ALL detected array models
        for array_model in all_array_models:
            nested_view = self._create_nested_view(
                model, view_name, array_model, view_label, dimension_generator, measure_generator, all_array_models
            )
            if nested_view is not None:  # Only add non-empty nested views
                views.append(nested_view)
