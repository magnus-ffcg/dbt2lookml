"""Comprehensive unit tests for _create_nested_view method."""

import pytest
from unittest.mock import Mock, patch

from dbt2lookml.generators.view import LookmlViewGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn
from dbt2lookml.models.column_collections import ColumnCollections


class TestCreateNestedViewComprehensive:
    """Comprehensive test coverage for _create_nested_view method."""
    
    @pytest.fixture
    def mock_cli_args(self):
        """Mock CLI arguments."""
        args = Mock()
        args.use_table_name = False
        return args
    
    @pytest.fixture
    def view_generator(self, mock_cli_args):
        """Create view generator instance."""
        return LookmlViewGenerator(mock_cli_args)
    
    @pytest.fixture
    def mock_model(self):
        """Mock DbtModel instance."""
        model = Mock(spec=DbtModel)
        model.unique_id = "model.test.test_model"
        model.name = "test_model"
        model.relation_name = "`project.dataset.test_table`"
        return model
    
    @pytest.fixture
    def mock_array_model(self):
        """Mock DbtModelColumn for array field."""
        array_model = Mock(spec=DbtModelColumn)
        array_model.name = "items"
        array_model.lookml_long_name = "items"
        return array_model
    
    @pytest.fixture
    def mock_dimension_generator(self):
        """Mock dimension generator."""
        generator = Mock()
        generator.lookml_dimensions_from_model.return_value = ([], {})
        generator.lookml_dimension_groups_from_model.return_value = {'dimension_groups': []}
        generator._comment_conflicting_dimensions.return_value = ([], [])
        generator._clean_dimension_groups_for_output.return_value = []
        return generator
    
    @pytest.fixture
    def mock_measure_generator(self):
        """Mock measure generator."""
        generator = Mock()
        generator.lookml_measures_from_model.return_value = []
        return generator
    
    @pytest.fixture
    def mock_column_collections(self):
        """Mock ColumnCollections."""
        collections = Mock(spec=ColumnCollections)
        collections.nested_view_columns = {'items': {'field1': Mock(), 'field2': Mock()}}
        return collections
    
    def test_create_nested_view_basic_functionality(self, view_generator, mock_model, mock_array_model, 
                                                   mock_dimension_generator, mock_measure_generator):
        """Test basic nested view creation functionality."""
        # Setup
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim', 'type': 'string'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label", 
                mock_dimension_generator, mock_measure_generator
            )
        
        # Assertions
        assert result is not None
        assert result['name'] == 'base__items'
        assert 'dimensions' in result
        assert result['dimensions'] == [{'name': 'test_dim', 'type': 'string'}]
    
    def test_create_nested_view_with_use_table_name(self, mock_cli_args, mock_model, mock_array_model,
                                                   mock_dimension_generator, mock_measure_generator):
        """Test nested view creation with use_table_name flag."""
        mock_cli_args.use_table_name = True
        view_generator = LookmlViewGenerator(mock_cli_args)
        
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            with patch('dbt2lookml.utils.camel_to_snake') as mock_camel_to_snake:
                mock_camel_to_snake.return_value = "test_table"
                
                result = view_generator._create_nested_view(
                    mock_model, "base", mock_array_model, "Test Label",
                    mock_dimension_generator, mock_measure_generator
                )
        
        assert result['name'] == 'test_table__items'
    
    def test_create_nested_view_with_dimension_groups(self, view_generator, mock_model, mock_array_model,
                                                     mock_dimension_generator, mock_measure_generator):
        """Test nested view creation with dimension groups."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = ([], {})
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{'name': 'created', 'type': 'time'}]
        }
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {'name': 'created', 'type': 'time'}
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'created_at': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        assert 'dimension_groups' in result
        assert result['dimension_groups'] == [{'name': 'created', 'type': 'time'}]
    
    def test_create_nested_view_with_measures(self, view_generator, mock_model, mock_array_model,
                                             mock_dimension_generator, mock_measure_generator):
        """Test nested view creation with measures."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = ([], {})
        mock_measure_generator.lookml_measures_from_model.return_value = [
            {'name': 'count', 'type': 'count'}
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'amount': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        assert 'measures' in result
        assert result['measures'] == [{'name': 'count', 'type': 'count'}]
    
    def test_create_nested_view_with_conflicting_dimensions(self, view_generator, mock_model, mock_array_model,
                                                           mock_dimension_generator, mock_measure_generator):
        """Test nested view creation with conflicting dimensions."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{'name': 'created', 'type': 'time'}]
        }
        mock_dimension_generator._comment_conflicting_dimensions.return_value = (
            [{'name': 'test_dim'}], ['conflicting_dim']
        )
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {'name': 'created', 'type': 'time'}
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # Check that conflicting dimensions comment is added
        comment_key = '# Removed conflicting dimensions: conflicting_dim'
        assert comment_key in result
        assert result[comment_key] == ""
    
    def test_create_nested_view_returns_none_for_empty_view(self, view_generator, mock_model, mock_array_model,
                                                           mock_dimension_generator, mock_measure_generator):
        """Test that empty nested views return None."""
        # Setup generators to return empty results
        mock_dimension_generator.lookml_dimensions_from_model.return_value = ([], {})
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {'dimension_groups': []}
        mock_measure_generator.lookml_measures_from_model.return_value = []
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        assert result is None
    
    def test_create_nested_view_with_array_models_none(self, view_generator, mock_model, mock_array_model,
                                                      mock_dimension_generator, mock_measure_generator):
        """Test nested view creation when array_models is None."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator, array_models=None
            )
        
        # Should call from_model with empty list when array_models is None
        mock_from_model.assert_called_once_with(mock_model, [])
        assert result is not None
    
    def test_create_nested_view_caching_behavior(self, view_generator, mock_model, mock_array_model,
                                                mock_dimension_generator, mock_measure_generator):
        """Test that column collections are cached properly."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            # First call
            view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
            
            # Second call with same parameters
            view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # from_model should only be called once due to caching
        assert mock_from_model.call_count == 1
    
    def test_create_nested_view_with_complex_array_models(self, view_generator, mock_model, mock_array_model,
                                                         mock_dimension_generator, mock_measure_generator):
        """Test nested view creation with complex array models list."""
        # Create mock array models with proper name attributes
        model1 = Mock()
        model1.name = 'model1'
        model2 = Mock()
        model2.name = 'model2'
        array_models = [model1, model2]
        
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator, array_models=array_models
            )
        
        mock_from_model.assert_called_once_with(mock_model, array_models)
        assert result is not None
    
    def test_create_nested_view_name_generation_edge_cases(self, view_generator, mock_model, mock_array_model,
                                                          mock_dimension_generator, mock_measure_generator):
        """Test edge cases in nested view name generation."""
        # Test with special characters in lookml_long_name
        mock_array_model.lookml_long_name = "special.field_name"
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        assert result['name'] == 'base__special.field_name'
    
    def test_create_nested_view_dimension_groups_cleaning(self, view_generator, mock_model, mock_array_model,
                                                         mock_dimension_generator, mock_measure_generator):
        """Test that dimension groups are properly cleaned for output."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = ([], {})
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{'name': 'created', 'type': 'time', 'extra_field': 'remove_me'}]
        }
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {'name': 'created', 'type': 'time'}  # cleaned version
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'created_at': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # Verify cleaning was called and result is cleaned
        mock_dimension_generator._clean_dimension_groups_for_output.assert_called_once()
        assert result['dimension_groups'] == [{'name': 'created', 'type': 'time'}]
    
    def test_create_nested_view_measures_with_walrus_operator(self, view_generator, mock_model, mock_array_model,
                                                             mock_dimension_generator, mock_measure_generator):
        """Test measures assignment using walrus operator."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = ([], {})
        mock_measure_generator.lookml_measures_from_model.return_value = [
            {'name': 'total_amount', 'type': 'sum'}
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'amount': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # Verify measures are called twice (once for empty check, once for assignment)
        assert mock_measure_generator.lookml_measures_from_model.call_count == 2
        assert 'measures' in result
        assert result['measures'] == [{'name': 'total_amount', 'type': 'sum'}]
    
    def test_create_nested_view_no_conflicting_dimensions_when_no_dimension_groups(self, view_generator, mock_model, 
                                                                                  mock_array_model, mock_dimension_generator, 
                                                                                  mock_measure_generator):
        """Test that conflict detection is skipped when no dimension groups exist."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {'dimension_groups': []}
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # Conflict detection should not be called when no dimension groups
        mock_dimension_generator._comment_conflicting_dimensions.assert_not_called()
        assert result is not None
    
    def test_create_nested_view_multiple_conflicting_dimensions(self, view_generator, mock_model, mock_array_model,
                                                               mock_dimension_generator, mock_measure_generator):
        """Test nested view with multiple conflicting dimensions."""
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{'name': 'created', 'type': 'time'}]
        }
        mock_dimension_generator._comment_conflicting_dimensions.return_value = (
            [{'name': 'test_dim'}], ['conflict1', 'conflict2', 'conflict3']
        )
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {'name': 'created', 'type': 'time'}
        ]
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator
            )
        
        # Check that all conflicting dimensions are listed in comment
        comment_key = '# Removed conflicting dimensions: conflict1,conflict2,conflict3'
        assert comment_key in result
    
    def test_create_nested_view_cache_key_generation(self, view_generator, mock_model, mock_array_model,
                                                    mock_dimension_generator, mock_measure_generator):
        """Test cache key generation with different array model configurations."""
        # Test with array models that have different attribute access patterns
        array_model_with_name = Mock()
        array_model_with_name.name = "model_with_name"
        
        array_model_without_name = Mock(spec=[])  # No name attribute
        
        array_models = [array_model_with_name, array_model_without_name]
        
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{'name': 'test_dim'}], {}
        )
        
        with patch.object(ColumnCollections, 'from_model') as mock_from_model:
            mock_collections = Mock()
            mock_collections.nested_view_columns = {'items': {'field1': Mock()}}
            mock_from_model.return_value = mock_collections
            
            result = view_generator._create_nested_view(
                mock_model, "base", mock_array_model, "Test Label",
                mock_dimension_generator, mock_measure_generator, array_models=array_models
            )
        
        # Should handle both cases: models with .name attribute and those without
        assert result is not None
        mock_from_model.assert_called_once_with(mock_model, array_models)
