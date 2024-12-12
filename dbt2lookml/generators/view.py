"""LookML view generator module."""

from typing import Dict, List, Optional

from dbt2lookml.models.dbt import DbtModel, DbtModelColumn


class LookmlViewGenerator:
    """LookML view generator."""

    def __init__(self, args):
        self._cli_args = args

    def _create_main_view(
        self,
        model: DbtModel,
        view_name: str,
        view_label: str,
        exclude_names: list,
        dimension_generator,
        measure_generator,
    ) -> dict:
        """Create the main view definition."""
        # Build view dict in specific order to match expected LookML output
        view = {
            'name': view_name,
            'label': view_label,
            'sql_table_name': model.relation_name,
        }

        # Add dimensions
        dimensions, nested_dimensions = dimension_generator.lookml_dimensions_from_model(
            model, exclude_names=exclude_names
        )
        if dimensions:
            view['dimensions'] = dimensions

        if dimension_groups := dimension_generator.lookml_dimension_groups_from_model(
            model, exclude_names=exclude_names
        ).get('dimension_groups'):
            view['dimension_groups'] = dimension_groups

        if measures := measure_generator.lookml_measures_from_model(
            model, exclude_names=exclude_names
        ):
            view['measures'] = measures

        if sets := dimension_generator.lookml_dimension_groups_from_model(
            model, exclude_names=exclude_names
        ).get('dimension_group_sets'):
            view['sets'] = sets

        return view

    def _create_nested_view(
        self,
        model: DbtModel,
        base_name: str,
        array_model: DbtModelColumn,
        view_label: str,
        dimension_generator,
        measure_generator,
    ) -> dict:
        """Create a nested view definition for an array field."""
        # Use table name if flag is set
        if self._cli_args.use_table_name:
            nested_view_name = f"{model.relation_name.split('.')[-1].strip('`')}__{array_model.name.replace('.', '__')}"
        else:
            nested_view_name = f"{base_name}__{array_model.name.replace('.', '__')}"

        include_names = [array_model.name]
        for col in model.columns.values():
            if col.name.startswith(f"{array_model.name}."):
                include_names.append(col.name)

        dimensions, nested_dimensions = dimension_generator.lookml_dimensions_from_model(
            model, include_names=include_names
        )
        nested_view = {'name': nested_view_name, 'label': view_label}
        if dimensions:
            nested_view['dimensions'] = dimensions

        if dimension_groups := dimension_generator.lookml_dimension_groups_from_model(
            model, include_names=include_names
        ).get('dimension_groups'):
            nested_view['dimension_groups'] = dimension_groups

        if measures := measure_generator.lookml_measures_from_model(
            model, include_names=include_names
        ):
            nested_view['measures'] = measures

        if sets := dimension_generator.lookml_dimension_groups_from_model(
            model, include_names=include_names
        ).get('dimension_group_sets'):
            nested_view['sets'] = sets

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
            model, view_name, view_label, exclude_names, dimension_generator, measure_generator
        )

        views = [main_view]

        for array_model in array_models:
            nested_view = self._create_nested_view(
                model, view_name, array_model, view_label, dimension_generator, measure_generator
            )
            views.append(nested_view)

        return views
