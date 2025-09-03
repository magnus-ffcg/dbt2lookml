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

    def _create_view_base(
        self,
        model: DbtModel,
        view_name: str,
        columns_subset: dict,
        array_models: list,
        dimension_generator,
        measure_generator,
        is_nested_view: bool = False,
        array_model_name: str = None,
    ) -> dict:
        """Shared view creation logic for both main and nested views."""
        # Use provided array models or empty list if none provided
        if array_models is None:
            array_models = []
        
        # Cache column collections to avoid repeated expensive processing
        array_model_names = [getattr(am, 'name', str(am)) for am in array_models]
        cache_key = (model.unique_id, tuple(sorted(array_model_names)))
        if cache_key not in self._column_collections_cache:
            self._column_collections_cache[cache_key] = ColumnCollections.from_model(model, array_models)
        
        # Generate dimensions
        dimension_kwargs = {'model': model, 'columns_subset': columns_subset}
        if is_nested_view:
            dimension_kwargs.update({'is_nested_view': True, 'array_model_name': array_model_name})
        
        dimensions, nested_dimensions = dimension_generator.lookml_dimensions_from_model(**dimension_kwargs)
        
        # Get dimension groups
        dimension_groups_result = dimension_generator.lookml_dimension_groups_from_model(**dimension_kwargs)
        dimension_groups = dimension_groups_result.get('dimension_groups', [])
        
        # Apply conflict detection
        conflicting_dimensions = []
        if dimensions and dimension_groups:
            dimensions = dimension_generator._comment_conflicting_dimensions(
                dimensions, dimension_groups, model.name
            )
        
        # Clean dimension groups for output
        if dimension_groups:
            dimension_groups = dimension_generator._clean_dimension_groups_for_output(dimension_groups)
        
        # Generate measures
        measures = measure_generator.lookml_measures_from_model(
            model, columns_subset=columns_subset
        )
        
        # Build base view structure
        view = {'name': view_name}
        
        if dimensions:
            view['dimensions'] = dimensions
        if dimension_groups:
            view['dimension_groups'] = dimension_groups
        if measures:
            view['measures'] = measures
        
        return view, nested_dimensions, measures

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
        # Get main view columns
        collections = self._get_column_collections(model, array_models)
        
        # Use shared base method
        view, nested_dimensions, measures = self._create_view_base(
            model, view_name.lower(), collections.main_view_columns, array_models,
            dimension_generator, measure_generator
        )
        
        # Main view specific properties
        view['sql_table_name'] = model.relation_name
        
        # Add nested array dimensions to main view
        if nested_dimensions:
            if 'dimensions' not in view:
                view['dimensions'] = []
            view['dimensions'].extend(nested_dimensions)
        
        # Add default count measure if no measures exist
        if not measures:
            view['measures'] = [{'name': 'count', 'type': 'count'}]
        
        # Add hidden flag if specified in meta
        if hidden := model._get_meta_looker('view', 'hidden'):
            view['hidden'] = 'yes' if hidden else 'no'
        
        return view

    def _generate_model_header_comment(self, model: DbtModel) -> str:
        """Generate header comment with dbt model metadata.
        
        Args:
            model: DbtModel to extract metadata from
            
        Returns:
            Multi-line comment string with model metadata
        """
        lines = []
        
        # Check if model has name attribute before accessing
        if hasattr(model, 'name'):
            lines.append(f"# Model: {getattr(model, 'name', '')}")
        
        # Check if model has description before accessing
        if hasattr(model, 'description'):
            lines.append(f"# Description: {getattr(model, 'description', '')}")
        
        if hasattr(model, 'tags'):
            lines.append(f"# Description: {','.join(getattr(model, 'tags', []))}")
        
        # Check for metadata stored in various possible locations
        # Try to get metadata from model attributes or stored manifest data
        manifest_data = getattr(model, '_manifest_data', None)
        if manifest_data:
            # Extract meta fields from stored manifest data
            meta = manifest_data.get('meta', {})
            
            #owner = meta.get('owner')
            if hasattr(meta, 'owner'):
                lines.append(f"# Owner: {getattr(model, 'owner', '')}")
            
            #maturity = meta.get('model_maturity')  
            if hasattr(meta, 'maturity'):
                lines.append(f"# Maturity: {getattr(model, 'maturity', '')}")
            
            #contains_pii = meta.get('contains_pii')
            if hasattr(meta, 'contains_pii'):
                pii_value = "Yes" if getattr(model, 'maturity', '') else "No"
                lines.append(f"# PII: {pii_value}")
            
            # Get group from manifest data
            #group = manifest_data.get('group')
            if hasattr(meta, 'group'):
                lines.append(f"# Group: {getattr(model, 'group', '')}")
             
        if len(lines) > 1:
            return '\n'.join(lines) + "\n\n"    
        else:
            # No content at all, return empty string
            return ""

    def _get_column_collections(self, model: DbtModel, array_models: list) -> ColumnCollections:
        """Get column collections with caching."""
        if array_models is None:
            array_models = []
        
        array_model_names = [getattr(am, 'name', str(am)) for am in array_models]
        cache_key = (model.unique_id, tuple(sorted(array_model_names)))
        if cache_key not in self._column_collections_cache:
            self._column_collections_cache[cache_key] = ColumnCollections.from_model(model, array_models)
        return self._column_collections_cache[cache_key]

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
        nested_view_name = self._generate_nested_view_name(model, base_name, array_model)
        
        # Get column collections for this nested view
        nested_columns = self._get_nested_view_columns(model, array_model, array_models)
        
        # Use shared base method
        view, _, measures = self._create_view_base(
            model, nested_view_name, nested_columns, array_models,
            dimension_generator, measure_generator,
            is_nested_view=True, array_model_name=array_model.name
        )
        
        # Check if view has content, return None if empty
        if self._is_empty_view(view, measures):
            return None
        
        return view

    def _generate_nested_view_name(self, model: DbtModel, base_name: str, array_model: DbtModelColumn) -> str:
        """Generate the nested view name based on CLI arguments."""
        if self._cli_args.use_table_name:
            from dbt2lookml.utils import camel_to_snake
            array_model_name = array_model.lookml_long_name
            table_name = model.relation_name.split('.')[-1].strip('`')
            base_name = camel_to_snake(table_name)
            return f"{base_name}__{array_model_name}"
        else:
            return f"{base_name}__{array_model.lookml_long_name}".lower()

    def _get_nested_view_columns(self, model: DbtModel, array_model: DbtModelColumn, array_models: list) -> dict:
        """Get column collections for the nested view with caching."""
        collections = self._get_column_collections(model, array_models)
        return collections.nested_view_columns.get(array_model.name, {})

    def _is_empty_view(self, view: dict, measures: list) -> bool:
        """Check if view would be empty (no content)."""
        has_dimensions = bool(view.get('dimensions'))
        has_dimension_groups = bool(view.get('dimension_groups'))
        has_measures = bool(measures)
        
        return not (has_dimensions or has_dimension_groups or has_measures)

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
