from typing import Any, Dict, List, Optional

from dbt2lookml.enums import LookerMeasureType, LookerScalarTypes
from dbt2lookml.generators.utils import get_column_name, map_bigquery_to_looker
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn
from dbt2lookml.models.looker import DbtMetaLookerMeasure


class LookmlMeasureGenerator:
    """Lookml dimension generator."""

    def __init__(self, args):
        self._cli_args = args

    def _apply_measure_attributes(self, measure_dict: Dict[str, Any], measure: DbtMetaLookerMeasure) -> None:
        """Apply measure attributes to the measure dictionary."""
        direct_attributes = [
            'approximate',
            'approximate_threshold',
            'can_filter',
            'tags',
            'alias',
            'convert_tz',
            'suggestable',
            'precision',
            'percentile',
            'group_label',
            'label',
            'description',
        ]
        for attr in direct_attributes:
            value = getattr(measure, attr, None)
            if value is not None:
                measure_dict[attr] = value
        # Special handling for value_format_name which is an enum
        if measure.value_format_name is not None:
            measure_dict['value_format_name'] = measure.value_format_name.value
        # Special handling for hidden attribute
        if measure.hidden is not None:
            measure_dict['hidden'] = 'yes' if measure.hidden else 'no'

    def _lookml_measure(
        self,
        column: DbtModelColumn,
        measure: DbtMetaLookerMeasure,
        table_format_sql: bool,
        model: DbtModel,
    ) -> Dict[str, Any]:
        """Create a LookML measure from a DBT model column and measure."""
        if not self._is_valid_measure_type(measure):
            return {}

        base_measure = self._create_base_measure(column, measure, table_format_sql, model)
        self._apply_measure_attributes(base_measure, measure)
        self._add_sql_distinct_key(base_measure, measure)
        self._add_measure_filters(base_measure, measure)

        return base_measure

    def _is_valid_measure_type(self, measure: DbtMetaLookerMeasure) -> bool:
        """Check if the measure type is valid.

        Args:
            measure: The measure to validate

        Returns:
            True if measure type is valid, False otherwise
        """
        return measure.type.value in LookerMeasureType.values()

    def _create_base_measure(
        self, column: DbtModelColumn, measure: DbtMetaLookerMeasure, table_format_sql: bool, model: DbtModel
    ) -> Dict[str, Any]:
        """Create the base measure dictionary with core attributes.

        Args:
            column: The column to create measure for
            measure: The measure configuration
            table_format_sql: Whether to use table format in SQL
            model: The DBT model

        Returns:
            Base measure dictionary with name, type, sql, and description
        """
        return {
            'name': f'm_{measure.type.value}_{column.name}',
            'type': measure.type.value,
            'sql': get_column_name(column, table_format_sql, getattr(model, '_catalog_data', None), model.unique_id),
            'description': measure.description or f'{measure.type.value} of {column.name}',
        }

    def _add_sql_distinct_key(self, measure_dict: Dict[str, Any], measure: DbtMetaLookerMeasure) -> None:
        """Add SQL distinct key to measure if specified.

        Args:
            measure_dict: The measure dictionary to modify
            measure: The measure configuration
        """
        if measure.sql_distinct_key is not None:
            measure_dict['sql_distinct_key'] = measure.sql_distinct_key

    def _add_measure_filters(self, measure_dict: Dict[str, Any], measure: DbtMetaLookerMeasure) -> None:
        """Add filters to measure if specified.

        Args:
            measure_dict: The measure dictionary to modify
            measure: The measure configuration
        """
        if measure.filters:
            measure_dict['filters'] = [{'field': f.filter_dimension, 'value': f.filter_expression} for f in measure.filters]

    def lookml_measures_from_model(
        self,
        model: DbtModel,
        columns_subset: Dict[str, DbtModelColumn],
    ) -> List[Dict[str, Any]]:
        """Generate measures from model using pre-filtered columns."""
        lookml_measures: List[Dict[str, Any]] = []
        table_format_sql = True
        for column in columns_subset.values():
            if (
                map_bigquery_to_looker(column.data_type) in LookerScalarTypes.values()
                and hasattr(column.meta, 'looker')
                and hasattr(column.meta.looker, 'measures')
                and column.meta.looker.measures
            ):
                lookml_measures.extend(
                    self._lookml_measure(column, measure, table_format_sql, model) for measure in column.meta.looker.measures
                )
        return lookml_measures
