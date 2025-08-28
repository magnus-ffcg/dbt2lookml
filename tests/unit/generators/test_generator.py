"""Tests for generators/__init__.py LookmlGenerator class."""

import pytest
from argparse import Namespace
from unittest.mock import Mock, patch
from dbt2lookml.generators import LookmlGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn, DbtModelMeta
from dbt2lookml.models.looker import DbtMetaLooker, DbtMetaLookerBase


@pytest.fixture
def cli_args():
    """Create CLI args fixture."""
    return Namespace(
        use_table_name=False,
        build_explore=False,
        skip_explore=False,
        include_models=[],
        exclude_models=[],
        target_dir='output',
        sql_table_name_format='{schema}.{table}',
        view_label_format='{model_name}',
        explore_label_format='{model_name}',
        dimension_group_format='{dimension_name}',
        measure_format='{measure_name}',
        join_format='{join_name}',
        filter_format='{filter_name}',
        parameter_format='{parameter_name}',
        set_format='{set_name}',
        datagroup_format='{datagroup_name}',
        access_grant_format='{access_grant_name}',
        connection_format='{connection_name}',
        include_format='{include_name}',
        dashboard_format='{dashboard_name}',
        lookml_format='{lookml_name}',
        model_format='{model_name}',
        view_format='{view_name}',
        explore_format='{explore_name}',
        dimension_format='{dimension_name}',
        filter_dimension_format='{filter_dimension_name}',
        measure_dimension_format='{measure_dimension_name}',
        parameter_dimension_format='{parameter_dimension_name}',
        set_dimension_format='{set_dimension_name}',
        datagroup_dimension_format='{datagroup_dimension_name}',
        access_grant_dimension_format='{access_grant_dimension_name}',
        connection_dimension_format='{connection_dimension_name}',
        include_dimension_format='{include_dimension_name}',
        dashboard_dimension_format='{dashboard_dimension_name}',
        lookml_dimension_format='{lookml_dimension_name}',
        model_dimension_format='{model_dimension_name}',
        view_dimension_format='{view_dimension_name}',
        explore_dimension_format='{explore_dimension_name}'
    )


@pytest.fixture
def sample_model():
    """Create a sample DbtModel for testing."""
    return DbtModel(
        unique_id='model.test.sample_model',
        name='sample_model',
        relation_name='`project.dataset.sample_table`',
        schema='dataset',
        description='Test model',
        tags=[],
        path='models/sample_model.sql',
        columns={
            'id': DbtModelColumn(
                name='id',
                data_type='INTEGER',
                description='Primary key'
            ),
            'name': DbtModelColumn(
                name='name',
                data_type='STRING',
                description='Name field'
            ),
            'created_at': DbtModelColumn(
                name='created_at',
                data_type='TIMESTAMP',
                description='Creation timestamp'
            )
        },
        meta=DbtModelMeta(looker=DbtMetaLooker())
    )


@pytest.fixture
def array_model():
    """Create a model with array columns for testing."""
    return DbtModel(
        unique_id='model.test.array_model',
        name='array_model',
        relation_name='`project.dataset.array_table`',
        schema='dataset',
        description='Test model with arrays',
        tags=[],
        path='models/array_model.sql',
        columns={
            'id': DbtModelColumn(
                name='id',
                data_type='INTEGER',
                description='Primary key'
            ),
            'tags': DbtModelColumn(
                name='tags',
                data_type='ARRAY<STRING>',
                description='Array of tags'
            ),
            'tags.value': DbtModelColumn(
                name='tags.value',
                data_type='STRING',
                description='Tag value'
            ),
            'metadata': DbtModelColumn(
                name='metadata',
                data_type='STRUCT',
                description='Metadata struct'
            ),
            'metadata.key': DbtModelColumn(
                name='metadata.key',
                data_type='STRING',
                description='Metadata key'
            ),
            'nested_array': DbtModelColumn(
                name='nested_array',
                data_type='ARRAY<STRUCT>',
                description='Nested array struct'
            ),
            'nested_array.field': DbtModelColumn(
                name='nested_array.field',
                data_type='STRING',
                description='Nested field'
            )
        },
        meta=DbtModelMeta(looker=DbtMetaLooker())
    )


class TestLookmlGenerator:
    """Test cases for LookmlGenerator class."""
    
    def test_init(self, cli_args):
        """Test LookmlGenerator initialization."""
        generator = LookmlGenerator(cli_args)
        
        assert generator._cli_args == cli_args
        assert generator.dimension_generator is not None
        assert generator.view_generator is not None
        assert generator.explore_generator is not None
        assert generator.measure_generator is not None
    
    def test_get_view_label_with_looker_meta(self, cli_args):
        """Test _get_view_label with looker meta label."""
        generator = LookmlGenerator(cli_args)
        
        # Create model with looker meta label
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker(view=DbtMetaLookerBase(label='Custom Label')))
        )
        
        result = generator._get_view_label(model)
        assert result == 'Custom Label'
    
    def test_get_view_label_fallback_to_name(self, cli_args):
        """Test _get_view_label fallback to model name."""
        generator = LookmlGenerator(cli_args)
        
        # Create model without looker meta label
        model = DbtModel(
            unique_id='model.test.test_model',
            name='test_model_name',
            relation_name='test_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/test_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        result = generator._get_view_label(model)
        assert result == 'Test Model Name'
    
    def test_get_view_label_no_name(self, cli_args):
        """Test _get_view_label when model has no name."""
        generator = LookmlGenerator(cli_args)
        
        # Create model without name
        model = Mock()
        model.meta = Mock()
        model.meta.looker = Mock()
        model.meta.looker.view = None  # No view metadata
        del model.name  # Remove name attribute
        
        result = generator._get_view_label(model)
        assert result is None
    
    def test_extract_array_models(self, cli_args, array_model):
        """Test _extract_array_models method."""
        generator = LookmlGenerator(cli_args)
        
        columns = list(array_model.columns.values())
        array_models = generator._extract_array_models(columns)
        
        # Should find columns with ARRAY in data_type
        array_names = [col.name for col in array_models]
        assert 'tags' in array_names
        assert 'nested_array' in array_names
        assert 'id' not in array_names  # INTEGER should not be included
    
    def test_extract_array_models_empty_list(self, cli_args):
        """Test _extract_array_models with empty column list."""
        generator = LookmlGenerator(cli_args)
        
        result = generator._extract_array_models([])
        assert result == []
    
    def test_extract_array_models_no_arrays(self, cli_args, sample_model):
        """Test _extract_array_models with no array columns."""
        generator = LookmlGenerator(cli_args)
        
        columns = list(sample_model.columns.values())
        result = generator._extract_array_models(columns)
        assert result == []
    
    def test_get_excluded_array_names(self, cli_args, array_model):
        """Test _get_excluded_array_names method."""
        generator = LookmlGenerator(cli_args)
        
        array_models = generator._extract_array_models(list(array_model.columns.values()))
        excluded_names = generator._get_excluded_array_names(array_model, array_models)
        
        # Should exclude nested fields from array models
        assert 'tags.value' in excluded_names
        assert 'nested_array.field' in excluded_names
        # metadata is just STRUCT (not ARRAY<STRUCT>), so metadata.key should not be excluded
    
    def test_get_excluded_array_names_no_arrays(self, cli_args, sample_model):
        """Test _get_excluded_array_names with no array models."""
        generator = LookmlGenerator(cli_args)
        
        excluded_names = generator._get_excluded_array_names(sample_model, [])
        assert excluded_names == []
    
    def test_get_file_path_use_table_name(self, cli_args, sample_model):
        """Test _get_file_path with use_table_name=True."""
        cli_args.use_table_name = True
        generator = LookmlGenerator(cli_args)
        
        result = generator._get_file_path(sample_model, 'test_view')
        expected = 'models/sample_table.view.lkml'
        assert result == expected
    
    def test_get_file_path_use_model_name(self, cli_args, sample_model):
        """Test _get_file_path with use_table_name=False."""
        cli_args.use_table_name = False
        generator = LookmlGenerator(cli_args)
        
        result = generator._get_file_path(sample_model, 'test_view')
        expected = 'models//test_view.view.lkml'
        assert result == expected
    
    def test_get_file_path_complex_relation_name(self, cli_args):
        """Test _get_file_path with complex relation name."""
        cli_args.use_table_name = True
        generator = LookmlGenerator(cli_args)
        
        model = DbtModel(
            unique_id='model.test.complex_model',
            name='complex_model',
            relation_name='`project-123.dataset_name.table_name_with_underscores`',
            schema='dataset_name',
            description='Test model',
            tags=[],
            path='models/subfolder/complex_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        result = generator._get_file_path(model, 'test_view')
        expected = 'models/subfolder/table_name_with_underscores.view.lkml'
        assert result == expected
    
    @patch('dbt2lookml.generators.LookmlViewGenerator')
    @patch('dbt2lookml.generators.LookmlExploreGenerator')
    def test_generate_without_explore(self, mock_explore_gen, mock_view_gen, cli_args, sample_model):
        """Test generate method without explore generation."""
        cli_args.build_explore = False
        
        # Mock the view generator
        mock_view_instance = Mock()
        mock_view_instance.generate.return_value = [{'name': 'test_view'}]
        mock_view_gen.return_value = mock_view_instance
        
        generator = LookmlGenerator(cli_args)
        generator.view_generator = mock_view_instance
        
        file_path, lookml = generator.generate(sample_model)
        
        assert 'view' in lookml
        assert 'explore' not in lookml
        assert file_path.endswith('.view.lkml')
    
    @patch('dbt2lookml.generators.LookmlViewGenerator')
    @patch('dbt2lookml.generators.LookmlExploreGenerator')
    def test_generate_with_explore(self, mock_explore_gen, mock_view_gen, cli_args, sample_model):
        """Test generate method with explore generation."""
        cli_args.build_explore = True
        
        # Mock the generators
        mock_view_instance = Mock()
        mock_view_instance.generate.return_value = [{'name': 'test_view'}]
        mock_view_gen.return_value = mock_view_instance
        
        mock_explore_instance = Mock()
        mock_explore_instance.generate.return_value = {'name': 'test_explore'}
        mock_explore_gen.return_value = mock_explore_instance
        
        generator = LookmlGenerator(cli_args)
        generator.view_generator = mock_view_instance
        generator.explore_generator = mock_explore_instance
        
        file_path, lookml = generator.generate(sample_model)
        
        assert 'view' in lookml
        assert 'explore' in lookml
        assert lookml['explore'] == {'name': 'test_explore'}
    
    @patch('dbt2lookml.generators.LookmlViewGenerator')
    def test_generate_with_table_name(self, mock_view_gen, cli_args, sample_model):
        """Test generate method with use_table_name=True."""
        cli_args.use_table_name = True
        cli_args.build_explore = False
        
        mock_view_instance = Mock()
        mock_view_instance.generate.return_value = [{'name': 'sample_table'}]
        mock_view_gen.return_value = mock_view_instance
        
        generator = LookmlGenerator(cli_args)
        generator.view_generator = mock_view_instance
        
        file_path, lookml = generator.generate(sample_model)
        
        # Should use table name from relation_name
        mock_view_instance.generate.assert_called_once()
        call_args = mock_view_instance.generate.call_args
        assert call_args[1]['view_name'] == 'sample_table'
    
    @patch('dbt2lookml.generators.LookmlViewGenerator')
    def test_generate_with_model_name(self, mock_view_gen, cli_args, sample_model):
        """Test generate method with use_table_name=False."""
        cli_args.use_table_name = False
        cli_args.build_explore = False
        
        mock_view_instance = Mock()
        mock_view_instance.generate.return_value = [{'name': 'sample_model'}]
        mock_view_gen.return_value = mock_view_instance
        
        generator = LookmlGenerator(cli_args)
        generator.view_generator = mock_view_instance
        
        file_path, lookml = generator.generate(sample_model)
        
        # Should use model name
        mock_view_instance.generate.assert_called_once()
        call_args = mock_view_instance.generate.call_args
        assert call_args[1]['view_name'] == 'sample_model'
    
    @patch('dbt2lookml.generators.LookmlViewGenerator')
    def test_generate_with_array_models(self, mock_view_gen, cli_args, array_model):
        """Test generate method with array models."""
        cli_args.build_explore = False
        
        mock_view_instance = Mock()
        mock_view_instance.generate.return_value = [{'name': 'array_model'}]
        mock_view_gen.return_value = mock_view_instance
        
        generator = LookmlGenerator(cli_args)
        generator.view_generator = mock_view_instance
        
        file_path, lookml = generator.generate(array_model)
        
        # Verify that array models and exclude names are passed
        mock_view_instance.generate.assert_called_once()
        call_args = mock_view_instance.generate.call_args
        
        assert 'array_models' in call_args[1]
        assert 'exclude_names' in call_args[1]
        
        # Should have array models
        array_models = call_args[1]['array_models']
        assert len(array_models) > 0
        
        # Should have exclude names for nested fields
        exclude_names = call_args[1]['exclude_names']
        assert len(exclude_names) > 0
    
    def test_generate_view_label_with_meta(self, cli_args):
        """Test generate method uses custom view label from meta."""
        cli_args.build_explore = False
        
        # Create model with custom label
        model = DbtModel(
            unique_id='model.test.labeled_model',
            name='labeled_model',
            relation_name='labeled_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/labeled_model.sql',
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker(view=DbtMetaLookerBase(label='Custom View Label')))
        )
        
        with patch('dbt2lookml.generators.LookmlViewGenerator') as mock_view_gen:
            mock_view_instance = Mock()
            mock_view_instance.generate.return_value = [{'name': 'labeled_model'}]
            mock_view_gen.return_value = mock_view_instance
            
            generator = LookmlGenerator(cli_args)
            generator.view_generator = mock_view_instance
            
            file_path, lookml = generator.generate(model)
            
            # Verify custom label is passed
            call_args = mock_view_instance.generate.call_args
            assert call_args[1]['view_label'] == 'Custom View Label'


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_extract_array_models_with_none_data_type(self, cli_args):
        """Test _extract_array_models with None data_type."""
        generator = LookmlGenerator(cli_args)
        
        columns = [
            DbtModelColumn(name='col1', data_type=None, description='Column with None type'),
            DbtModelColumn(name='col2', data_type='ARRAY<STRING>', description='Array column'),
        ]
        
        result = generator._extract_array_models(columns)
        assert len(result) == 1
        assert result[0].name == 'col2'
    
    def test_get_excluded_array_names_struct_without_children(self, cli_args):
        """Test _get_excluded_array_names with STRUCT that has no children."""
        generator = LookmlGenerator(cli_args)
        
        model = DbtModel(
            unique_id='model.test.struct_model',
            name='struct_model',
            relation_name='struct_table',
            schema='test_schema',
            description='Test model',
            tags=[],
            path='models/struct_model.sql',
            columns={
                'standalone_struct': DbtModelColumn(
                    name='standalone_struct',
                    data_type='STRUCT',
                    description='STRUCT without children'
                )
            },
            meta=DbtModelMeta(looker=DbtMetaLooker())
        )
        
        excluded_names = generator._get_excluded_array_names(model, [])
        
        # STRUCT without children should not be excluded
        assert 'standalone_struct' not in excluded_names
