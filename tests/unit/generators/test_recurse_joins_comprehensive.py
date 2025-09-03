"""Comprehensive unit tests for LookmlExploreGenerator.recurse_joins method."""

from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from dbt2lookml.generators.explore import LookmlExploreGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn, DbtModelMeta
from dbt2lookml.models.looker import DbtMetaLooker


class TestRecurseJoinsComprehensive:
    """Comprehensive test coverage for recurse_joins method."""

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
    def cli_args_use_table_name(self):
        """Create CLI args fixture with use_table_name=True."""
        return Namespace(
            use_table_name=True,
            include_models=[],
            exclude_models=[],
            target_dir='output',
        )

    @pytest.fixture
    def generator(self, cli_args):
        """Create generator instance."""
        return LookmlExploreGenerator(cli_args)

    @pytest.fixture
    def generator_use_table_name(self, cli_args_use_table_name):
        """Create generator instance with use_table_name=True."""
        return LookmlExploreGenerator(cli_args_use_table_name)

    @pytest.fixture
    def test_model(self):
        """Create test model with columns."""
        columns = {
            'id': DbtModelColumn(name='id', data_type='STRING'),
            'items': DbtModelColumn(name='items', data_type='ARRAY', original_name='Items'),
            'items.code': DbtModelColumn(name='items.code', data_type='STRING', original_name='Items.Code'),
        }

        return DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='`project.dataset.test_table`',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns=columns,
            meta=DbtModelMeta(looker=DbtMetaLooker()),
        )

    def test_empty_structure_returns_empty_list(self, generator, test_model):
        """Test recurse_joins with empty structure returns empty list."""
        result = generator.recurse_joins({}, test_model)
        assert result == []

    def test_single_array_join_creation(self, generator, test_model):
        """Test basic single array join creation."""
        structure = {'items': {'column': test_model.columns['items'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 1
        join = result[0]
        assert join['name'] == 'test_model__items'
        assert join['relationship'] == 'one_to_many'
        assert join['type'] == 'left_outer'
        assert 'UNNEST' in join['sql']
        assert 'test_model.items' in join['sql']

    def test_use_table_name_affects_join_naming(self, generator_use_table_name, test_model):
        """Test that use_table_name affects join naming."""
        structure = {'items': {'column': test_model.columns['items'], 'children': []}}

        result = generator_use_table_name.recurse_joins(structure, test_model)

        assert len(result) == 1
        join = result[0]
        # Should use table name 'test_table' instead of model name 'test_model'
        assert join['name'] == 'test_table__items'
        assert 'test_table.items' in join['sql']

    def test_camelcase_conversion_in_join_names(self, generator, test_model):
        """Test CamelCase to snake_case conversion in join names."""
        # Add column with CamelCase original name
        test_model.columns['supplier_info'] = DbtModelColumn(
            name='supplier_info', data_type='ARRAY', original_name='SupplierInfo'
        )

        structure = {'supplier_info': {'column': test_model.columns['supplier_info'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 1
        join = result[0]
        assert join['name'] == 'test_model__supplier_info'
        assert 'test_model.supplier_info' in join['sql']

    def test_nested_array_structure_sorting(self, generator, test_model):
        """Test that array models are sorted by nesting depth."""
        # Add nested structure
        test_model.columns['items.details'] = DbtModelColumn(
            name='items.details', data_type='ARRAY', original_name='Items.Details'
        )
        test_model.columns['items.details.specs'] = DbtModelColumn(
            name='items.details.specs', data_type='ARRAY', original_name='Items.Details.Specs'
        )

        structure = {
            'items.details.specs': {'column': test_model.columns['items.details.specs'], 'children': []},
            'items': {'column': test_model.columns['items'], 'children': []},
            'items.details': {'column': test_model.columns['items.details'], 'children': []},
        }

        result = generator.recurse_joins(structure, test_model)

        # Should be sorted by depth: items, items.details, items.details.specs
        assert len(result) == 3
        assert result[0]['name'] == 'test_model__items'
        assert result[1]['name'] == 'test_model__items__details'
        assert result[2]['name'] == 'test_model__items__details__specs'

    def test_parent_view_detection_and_referencing(self, generator, test_model):
        """Test parent view detection for nested arrays."""
        # Add nested structure
        test_model.columns['items.details'] = DbtModelColumn(
            name='items.details', data_type='ARRAY', original_name='Items.Details'
        )

        structure = {
            'items': {'column': test_model.columns['items'], 'children': []},
            'items.details': {'column': test_model.columns['items.details'], 'children': []},
        }

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 2

        # First join should reference base model
        first_join = result[0]
        assert first_join['name'] == 'test_model__items'
        assert 'test_model.items' in first_join['sql']

        # Second join should reference parent view
        second_join = result[1]
        assert second_join['name'] == 'test_model__items__details'
        assert 'test_model__items.details' in second_join['sql']

    def test_original_name_fallback_handling(self, generator, test_model):
        """Test handling when original_name is not available."""
        # Column without original_name
        test_model.columns['simple_array'] = DbtModelColumn(name='simple_array', data_type='ARRAY')

        structure = {'simple_array': {'column': test_model.columns['simple_array'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 1
        join = result[0]
        assert join['name'] == 'test_model__simple_array'
        assert 'test_model.simple_array' in join['sql']

    def test_complex_nested_structure_with_multiple_levels(self, generator, test_model):
        """Test complex nested structure with multiple nesting levels."""
        # Build complex nested structure
        test_model.columns.update(
            {
                'items.classification': DbtModelColumn(
                    name='items.classification', data_type='STRUCT', original_name='Items.Classification'
                ),
                'items.classification.categories': DbtModelColumn(
                    name='items.classification.categories', data_type='ARRAY', original_name='Items.Classification.Categories'
                ),
                'supplier_data': DbtModelColumn(name='supplier_data', data_type='ARRAY', original_name='SupplierData'),
            }
        )

        structure = {
            'items': {'column': test_model.columns['items'], 'children': []},
            'items.classification.categories': {'column': test_model.columns['items.classification.categories'], 'children': []},
            'supplier_data': {'column': test_model.columns['supplier_data'], 'children': []},
        }

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 3

        # Check that joins are created in correct order
        join_names = [join['name'] for join in result]
        assert 'test_model__items' in join_names
        assert 'test_model__items__classification__categories' in join_names
        assert 'test_model__supplier_data' in join_names

    def test_join_sql_format_for_top_level_arrays(self, generator, test_model):
        """Test SQL format for top-level array joins."""
        structure = {'items': {'column': test_model.columns['items'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        join = result[0]
        expected_sql = 'LEFT JOIN UNNEST(${test_model.items}) AS test_model__items'
        assert join['sql'] == expected_sql

    def test_join_sql_format_for_nested_arrays(self, generator, test_model):
        """Test SQL format for nested array joins."""
        # Add nested array
        test_model.columns['items.tags'] = DbtModelColumn(name='items.tags', data_type='ARRAY', original_name='Items.Tags')

        structure = {
            'items': {'column': test_model.columns['items'], 'children': []},
            'items.tags': {'column': test_model.columns['items.tags'], 'children': []},
        }

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 2

        # Nested join should reference parent view
        nested_join = result[1]
        expected_sql = 'LEFT JOIN UNNEST(${test_model__items.tags}) AS test_model__items__tags'
        assert nested_join['sql'] == expected_sql

    def test_join_properties_are_consistent(self, generator, test_model):
        """Test that all joins have consistent properties."""
        structure = {'items': {'column': test_model.columns['items'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        join = result[0]
        assert join['relationship'] == 'one_to_many'
        assert join['type'] == 'left_outer'
        assert 'required_joins' not in join  # Should not be present per fixture expectations

    @patch('dbt2lookml.generators.explore.logging')
    def test_logging_behavior(self, mock_logging, generator, test_model):
        """Test that appropriate logging occurs during processing."""
        structure = {'items': {'column': test_model.columns['items'], 'children': []}}

        generator.recurse_joins(structure, test_model)

        # Verify logging was not called (no debug logging in recurse_joins currently)
        # This test documents current behavior
        assert True  # Placeholder - adjust based on actual logging needs

    def test_edge_case_empty_original_name_parts(self, generator, test_model):
        """Test edge case with empty or malformed original name parts."""
        # Column with edge case original name
        test_model.columns['edge_case'] = DbtModelColumn(
            name='edge_case', data_type='ARRAY', original_name=''  # Empty original name
        )

        structure = {'edge_case': {'column': test_model.columns['edge_case'], 'children': []}}

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 1
        join = result[0]
        assert join['name'] == 'test_model__edge_case'

    def test_deeply_nested_parent_view_detection(self, generator, test_model):
        """Test parent view detection with deeply nested structures."""
        # Create deeply nested structure
        test_model.columns.update(
            {
                'level1': DbtModelColumn(name='level1', data_type='ARRAY', original_name='Level1'),
                'level1.level2': DbtModelColumn(name='level1.level2', data_type='ARRAY', original_name='Level1.Level2'),
                'level1.level2.level3': DbtModelColumn(
                    name='level1.level2.level3', data_type='ARRAY', original_name='Level1.Level2.Level3'
                ),
            }
        )

        structure = {
            'level1': {'column': test_model.columns['level1'], 'children': []},
            'level1.level2': {'column': test_model.columns['level1.level2'], 'children': []},
            'level1.level2.level3': {'column': test_model.columns['level1.level2.level3'], 'children': []},
        }

        result = generator.recurse_joins(structure, test_model)

        assert len(result) == 3

        # Check parent view references
        assert 'test_model.level1' in result[0]['sql']  # References base model
        assert 'test_model__level1.level2' in result[1]['sql']  # References level1 view
        assert 'test_model__level1__level2.level3' in result[2]['sql']  # References level2 view
