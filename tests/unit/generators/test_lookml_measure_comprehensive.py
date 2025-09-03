"""Comprehensive unit tests for LookmlMeasureGenerator._lookml_measure method."""

from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from dbt2lookml.enums import LookerMeasureType, LookerValueFormatName
from dbt2lookml.generators.measure import LookmlMeasureGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn
from dbt2lookml.models.looker import DbtMetaLookerMeasure, DbtMetaLookerMeasureFilter


class TestLookmlMeasureComprehensive:
    """Comprehensive test coverage for _lookml_measure method."""

    @pytest.fixture
    def cli_args(self):
        """Create CLI args fixture."""
        return Namespace(
            use_table_name=False,
            include_models=[],
            exclude_models=[],
            target_dir='output',
        )

    @pytest.fixture
    def generator(self, cli_args):
        """Create generator instance."""
        return LookmlMeasureGenerator(cli_args)

    @pytest.fixture
    def sample_column(self):
        """Create sample column fixture."""
        return DbtModelColumn(name='revenue', data_type='NUMERIC', description='Revenue amount')

    @pytest.fixture
    def sample_model(self):
        """Create sample model fixture."""
        model = Mock(spec=DbtModel)
        model.unique_id = 'model.test.sample_model'
        model._catalog_data = {'test': 'data'}
        return model

    @pytest.fixture
    def basic_measure(self):
        """Create basic measure fixture."""
        return DbtMetaLookerMeasure(type=LookerMeasureType.SUM, description='Sum of revenue')

    def test_invalid_measure_type_returns_empty_dict(self, generator, sample_column, sample_model):
        """Test _lookml_measure with invalid measure type returns empty dict."""
        invalid_measure = Mock()
        invalid_measure.type.value = 'invalid_type'

        with patch('dbt2lookml.generators.measure.LookerMeasureType.values', return_value=['sum', 'count', 'average']):
            result = generator._lookml_measure(sample_column, invalid_measure, True, sample_model)

        assert result == {}

    def test_basic_measure_creation(self, generator, sample_column, sample_model, basic_measure):
        """Test basic measure creation with minimal attributes."""
        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, basic_measure, True, sample_model)

        expected = {'name': 'm_sum_revenue', 'type': 'sum', 'sql': '${TABLE}.revenue', 'description': 'Sum of revenue'}

        assert result['name'] == expected['name']
        assert result['type'] == expected['type']
        assert result['sql'] == expected['sql']
        assert result['description'] == expected['description']

    def test_measure_with_default_description(self, generator, sample_column, sample_model):
        """Test measure creation with default description when none provided."""
        measure = DbtMetaLookerMeasure(type=LookerMeasureType.COUNT, description=None)

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        assert result['description'] == 'count of revenue'

    def test_measure_with_all_direct_attributes(self, generator, sample_column, sample_model):
        """Test measure creation with all direct attributes."""
        measure = DbtMetaLookerMeasure(
            type=LookerMeasureType.AVERAGE,
            description='Average revenue',
            precision=2,
            group_label='Revenue Metrics',
            label='Average Revenue',
        )

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        # Valid attributes should be present
        assert result['precision'] == 2
        assert result['group_label'] == 'Revenue Metrics'
        assert result['label'] == 'Average Revenue'
        assert result['description'] == 'Average revenue'

    def test_measure_with_value_format_name_enum(self, generator, sample_column, sample_model):
        """Test measure creation with value_format_name enum."""
        measure = DbtMetaLookerMeasure(
            type=LookerMeasureType.SUM, description='Sum with currency format', value_format_name=LookerValueFormatName.USD
        )

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        assert result['value_format_name'] == 'usd'

    def test_measure_with_hidden_true(self, generator, sample_column, sample_model):
        """Test measure creation with hidden=True."""
        measure = DbtMetaLookerMeasure(type=LookerMeasureType.SUM, description='Hidden sum', hidden=True)

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        assert result['hidden'] == 'yes'

    def test_measure_with_hidden_false(self, generator, sample_column, sample_model):
        """Test measure creation with hidden=False."""
        measure = DbtMetaLookerMeasure(type=LookerMeasureType.SUM, description='Visible sum', hidden=False)

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        assert result['hidden'] == 'no'

    def test_measure_with_sql_distinct_key(self, generator, sample_column, sample_model):
        """Test measure creation with sql_distinct_key."""
        measure = DbtMetaLookerMeasure(
            type=LookerMeasureType.COUNT_DISTINCT, description='Distinct count', sql_distinct_key='${TABLE}.id'
        )

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        assert result['sql_distinct_key'] == '${TABLE}.id'

    def test_measure_with_single_filter(self, generator, sample_column, sample_model):
        """Test measure creation with single filter."""
        filter_obj = DbtMetaLookerMeasureFilter(filter_dimension='status', filter_expression='active')
        measure = DbtMetaLookerMeasure(type=LookerMeasureType.SUM, description='Filtered sum', filters=[filter_obj])

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        expected_filters = [{'field': 'status', 'value': 'active'}]
        assert result['filters'] == expected_filters

    def test_measure_with_multiple_filters(self, generator, sample_column, sample_model):
        """Test measure creation with multiple filters."""
        filter1 = DbtMetaLookerMeasureFilter(filter_dimension='status', filter_expression='active')
        filter2 = DbtMetaLookerMeasureFilter(filter_dimension='region', filter_expression='US')
        measure = DbtMetaLookerMeasure(
            type=LookerMeasureType.COUNT, description='Multi-filtered count', filters=[filter1, filter2]
        )

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        expected_filters = [{'field': 'status', 'value': 'active'}, {'field': 'region', 'value': 'US'}]
        assert result['filters'] == expected_filters

    def test_measure_with_table_format_sql_false(self, generator, sample_column, sample_model, basic_measure):
        """Test measure creation with table_format_sql=False."""
        with patch('dbt2lookml.generators.measure.get_column_name', return_value='revenue') as mock_get_column:
            result = generator._lookml_measure(sample_column, basic_measure, False, sample_model)

            # Verify get_column_name was called with correct parameters
            mock_get_column.assert_called_once_with(sample_column, False, sample_model._catalog_data, sample_model.unique_id)
            assert result['sql'] == 'revenue'

    def test_measure_with_complex_column_name(self, generator, sample_model, basic_measure):
        """Test measure creation with complex column name."""
        complex_column = DbtModelColumn(name='nested.field.value', data_type='NUMERIC')

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.nested__field__value'):
            result = generator._lookml_measure(complex_column, basic_measure, True, sample_model)

        assert result['name'] == 'm_sum_nested.field.value'
        assert result['sql'] == '${TABLE}.nested__field__value'

    def test_measure_name_generation_different_types(self, generator, sample_column, sample_model):
        """Test measure name generation for different measure types."""
        measure_types = [
            (LookerMeasureType.SUM, 'm_sum_revenue'),
            (LookerMeasureType.COUNT, 'm_count_revenue'),
            (LookerMeasureType.AVERAGE, 'm_average_revenue'),
            (LookerMeasureType.MIN, 'm_min_revenue'),
            (LookerMeasureType.MAX, 'm_max_revenue'),
        ]

        for measure_type, expected_name in measure_types:
            measure = DbtMetaLookerMeasure(type=measure_type, description=f'{measure_type.value} measure')

            with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
                result = generator._lookml_measure(sample_column, measure, True, sample_model)

            assert result['name'] == expected_name

    def test_measure_with_none_attributes_skipped(self, generator, sample_column, sample_model):
        """Test that None attributes are not included in result."""
        measure = DbtMetaLookerMeasure(
            type=LookerMeasureType.SUM,
            description='Basic sum',
            approximate=None,
            tags=None,
            hidden=None,
            sql_distinct_key=None,
            filters=None,
        )

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        # These keys should not be present when values are None
        assert 'approximate' not in result
        assert 'tags' not in result
        assert 'hidden' not in result
        assert 'sql_distinct_key' not in result
        assert 'filters' not in result

    def test_measure_with_empty_filters_list(self, generator, sample_column, sample_model):
        """Test measure creation with empty filters list."""
        measure = DbtMetaLookerMeasure(type=LookerMeasureType.SUM, description='Sum without filters', filters=[])

        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            result = generator._lookml_measure(sample_column, measure, True, sample_model)

        # Empty filters list should not add filters key to result (falsy check)
        assert 'filters' not in result

    def test_apply_measure_attributes_called(self, generator, sample_column, sample_model, basic_measure):
        """Test that _apply_measure_attributes is called during measure creation."""
        with patch('dbt2lookml.generators.measure.get_column_name', return_value='${TABLE}.revenue'):
            with patch.object(generator, '_apply_measure_attributes') as mock_apply:
                result = generator._lookml_measure(sample_column, basic_measure, True, sample_model)

                # Verify _apply_measure_attributes was called
                mock_apply.assert_called_once()
                call_args = mock_apply.call_args[0]
                assert call_args[1] == basic_measure  # Second argument should be the measure

    def test_catalog_data_passed_to_get_column_name(self, generator, sample_column, sample_model, basic_measure):
        """Test that catalog data is properly passed to get_column_name."""
        sample_model._catalog_data = {'custom': 'catalog_data'}

        with patch('dbt2lookml.generators.measure.get_column_name') as mock_get_column:
            generator._lookml_measure(sample_column, basic_measure, True, sample_model)

            mock_get_column.assert_called_once_with(sample_column, True, {'custom': 'catalog_data'}, sample_model.unique_id)

    def test_model_without_catalog_data(self, generator, sample_column, basic_measure):
        """Test measure creation with model that has no catalog data."""
        model_no_catalog = Mock(spec=DbtModel)
        model_no_catalog.unique_id = 'model.test.no_catalog'
        # No _catalog_data attribute

        with patch('dbt2lookml.generators.measure.get_column_name') as mock_get_column:
            generator._lookml_measure(sample_column, basic_measure, True, model_no_catalog)

            mock_get_column.assert_called_once_with(
                sample_column, True, None, model_no_catalog.unique_id  # Should pass None when no catalog data
            )
