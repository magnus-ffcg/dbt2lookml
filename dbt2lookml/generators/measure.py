from dbt2lookml.models.dbt import DbtModel, DbtModelColumn
from dbt2lookml.models.looker import DbtMetaLookerMeasure
from dbt2lookml.generators.utils import map_bigquery_to_looker
from dbt2lookml.enums import LookerMeasureType, LookerScalarTypes
from dbt2lookml.generators.utils import get_column_name


class LookmlMeasureGenerator:
    """Lookml dimension generator."""

    def __init__(self, args):
        self._cli_args = args

    def _apply_measure_attributes(self, measure_dict: dict, measure: DbtMetaLookerMeasure) -> None:
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
    ) -> dict:
        """Create a LookML measure from a DBT model column and measure."""
        if measure.type.value not in [t.value for t in LookerMeasureType]:
            return None

        m = {
            'name': f'm_{measure.type.value}_{column.name}',
            'type': measure.type.value,
            'sql': get_column_name(column, table_format_sql),
            'description': measure.description or f'{measure.type.value} of {column.name}',
        }

        # Apply all measure attributes
        self._apply_measure_attributes(m, measure)

        # Handle SQL distinct key
        if measure.sql_distinct_key is not None:
            m['sql_distinct_key'] = measure.sql_distinct_key

        # Handle filters
        if measure.filters:
            m['filters'] = [
                {'field': f.filter_dimension, 'value': f.filter_expression} for f in measure.filters
            ]

        return m

    def lookml_measures_from_model(
        self, model: DbtModel, include_names: list = None, exclude_names: list = None
    ) -> list:
        """Generate measures from model."""
        if exclude_names is None:
            exclude_names = []
        lookml_measures = []
        table_format_sql = True

        for column in model.columns.values():
            if include_names:
                table_format_sql = False

                # For nested fields, if any parent is in include_names, include this field
                if column.name not in include_names and all(
                    parent not in include_names for parent in column.name.split('.')
                ):
                    continue

            if exclude_names and column.name in exclude_names:
                continue

            if (
                map_bigquery_to_looker(column.data_type) in LookerScalarTypes.values()
                and hasattr(column.meta, 'looker')
                and hasattr(column.meta.looker, 'measures')
                and column.meta.looker.measures
            ):
                lookml_measures.extend(
                    self._lookml_measure(column, measure, table_format_sql, model)
                    for measure in column.meta.looker.measures
                )

        return lookml_measures
