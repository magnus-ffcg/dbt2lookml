"""Test LookML View Generator implementations."""

import pytest
from argparse import Namespace
from unittest.mock import Mock, patch, MagicMock

from dbt2lookml.generators.view import LookmlViewGenerator
from dbt2lookml.models.dbt import (
    DbtModel,
    DbtModelColumn,
    DbtModelMeta,
    DbtResourceType,
)
from dbt2lookml.models.looker import DbtMetaLooker, DbtMetaLookerBase


@pytest.fixture
def cli_args():
    """Fixture for CLI arguments."""
    return Namespace(
        use_table_name=False,
        table_format_sql=True,
    )


@pytest.fixture
def sample_model():
    """Fixture for a sample DbtModel."""
    return DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.test_table`",
        columns={
            "id": DbtModelColumn(
                name="id",
                lookml_name="id",
                lookml_long_name="id",
                data_type="STRING",
                nested=False,
            ),
            "name": DbtModelColumn(
                name="name",
                lookml_name="name", 
                lookml_long_name="name",
                data_type="STRING",
                nested=False,
            ),
        },
        meta=DbtModelMeta(),
        unique_id="model.test.test_model",
        resource_type=DbtResourceType.MODEL,
        schema="test_schema",
        description="Test model",
        tags=[],
    )


@pytest.fixture
def mock_dimension_generator():
    """Mock dimension generator."""
    generator = Mock()
    generator.lookml_dimensions_from_model.return_value = (
        [{"name": "id", "type": "string"}],
        []  # nested dimensions
    )
    generator.lookml_dimension_groups_from_model.return_value = {
        'dimension_groups': [],
        'dimension_sets': []
    }
    generator._comment_conflicting_dimensions.return_value = (
        [{"name": "id", "type": "string"}],
        []  # conflicting dimensions
    )
    generator._clean_dimension_groups_for_output.return_value = []
    return generator


@pytest.fixture
def mock_measure_generator():
    """Mock measure generator."""
    generator = Mock()
    generator.lookml_measures_from_model.return_value = []
    return generator


class TestLookmlViewGenerator:
    """Test LookmlViewGenerator class."""

    def test_init(self, cli_args):
        """Test LookmlViewGenerator initialization."""
        generator = LookmlViewGenerator(cli_args)
        assert generator._cli_args == cli_args

    def test_create_main_view_basic(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with basic model."""
        generator = LookmlViewGenerator(cli_args)
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert result['name'] == 'test_view'
            assert result['sql_table_name'] == '`project.dataset.test_table`'
            assert 'dimensions' in result

    def test_create_main_view_with_array_models(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with array models."""
        generator = LookmlViewGenerator(cli_args)
        array_models = [Mock()]
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
                array_models=array_models,
            )
            
            mock_collections.assert_called_once_with(sample_model, array_models)

    def test_create_main_view_with_nested_dimensions(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with nested dimensions."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock nested dimensions
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{"name": "id", "type": "string"}],
            [{"name": "nested_field", "type": "string", "hidden": "yes"}]
        )
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert len(result['dimensions']) == 2
            assert any(d['name'] == 'nested_field' for d in result['dimensions'])

    def test_create_main_view_with_dimension_groups(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with dimension groups."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock dimension groups
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{"name": "created", "type": "time"}],
            'dimension_sets': []
        }
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {"name": "created", "type": "time"}
        ]
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert 'dimension_groups' in result
            assert len(result['dimension_groups']) == 1

    def test_create_main_view_with_measures(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with measures."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock measures
        mock_measure_generator.lookml_measures_from_model.return_value = [
            {"name": "count", "type": "count"}
        ]
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert 'measures' in result
            assert len(result['measures']) == 1

    def test_create_main_view_with_hidden_meta(self, cli_args, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with hidden meta attribute."""
        generator = LookmlViewGenerator(cli_args)
        
        # Create model with hidden meta
        model = DbtModel(
            name="test_model",
            path="models/test_model.sql",
            relation_name="`project.dataset.test_table`",
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker(view=DbtMetaLookerBase(hidden=True))),
            unique_id="model.test.test_model",
            resource_type=DbtResourceType.MODEL,
            schema="test_schema",
            description="Test model",
            tags=[],
        )
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = {}
            
            result = generator._create_main_view(
                model=model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert result['hidden'] == 'yes'

    def test_create_main_view_with_conflicting_dimensions(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view with conflicting dimensions."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock conflicting dimensions
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [{"name": "created", "type": "time"}],
            'dimension_sets': []
        }
        mock_dimension_generator._comment_conflicting_dimensions.return_value = (
            [{"name": "id", "type": "string"}],
            ["conflicting_field"]  # conflicting dimensions
        )
        mock_dimension_generator._clean_dimension_groups_for_output.return_value = [
            {"name": "created", "type": "time"}
        ]
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            # Should have comment about conflicting dimensions
            assert any('Removed conflicting dimensions' in key for key in result.keys())

    def test_is_yes_no_with_hidden_true(self, cli_args):
        """Test _is_yes_no returns 'yes' when model is hidden."""
        generator = LookmlViewGenerator(cli_args)
        
        model = DbtModel(
            name="test_model",
            path="models/test_model.sql",
            relation_name="`project.dataset.test_table`",
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker(view=DbtMetaLookerBase(hidden=True))),
            unique_id="model.test.test_model",
            resource_type=DbtResourceType.MODEL,
            schema="test_schema",
            description="Test model",
            tags=[],
        )
        
        result = generator._is_yes_no(model)
        assert result == 'yes'

    def test_is_yes_no_with_hidden_false(self, cli_args):
        """Test _is_yes_no returns 'no' when model is not hidden."""
        generator = LookmlViewGenerator(cli_args)
        
        model = DbtModel(
            name="test_model",
            path="models/test_model.sql",
            relation_name="`project.dataset.test_table`",
            columns={},
            meta=DbtModelMeta(looker=DbtMetaLooker(view=DbtMetaLookerBase(hidden=False))),
            unique_id="model.test.test_model",
            resource_type=DbtResourceType.MODEL,
            schema="test_schema",
            description="Test model",
            tags=[],
        )
        
        result = generator._is_yes_no(model)
        assert result == 'no'

    def test_is_yes_no_with_no_meta(self, cli_args, sample_model):
        """Test _is_yes_no returns 'no' when model has no meta."""
        generator = LookmlViewGenerator(cli_args)
        result = generator._is_yes_no(sample_model)
        assert result == 'no'

    def test_create_nested_view(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_nested_view method."""
        generator = LookmlViewGenerator(cli_args)
        
        array_model = DbtModelColumn(
            name="items",
            lookml_name="items",
            lookml_long_name="items",
            data_type="ARRAY<STRUCT<name STRING>>",
            nested=False,
        )
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.nested_view_columns = {
                "items.name": DbtModelColumn(
                    name="items.name",
                    lookml_name="items__name",
                    lookml_long_name="items__name", 
                    data_type="STRING",
                    nested=True,
                )
            }
            
            result = generator._create_nested_view(
                model=sample_model,
                base_name="test_model",
                array_model=array_model,
                view_label="Test View",
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
                array_models=[],
            )
            
            assert result['name'] == 'test_model__items'
            assert 'name' in result

    @patch('dbt2lookml.generators.view.LookmlViewGenerator._create_main_view')
    @patch('dbt2lookml.generators.view.LookmlViewGenerator._create_nested_view')
    def test_generate_with_array_models(self, mock_create_nested, mock_create_main, cli_args, sample_model):
        """Test generate method with array models."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock return values
        mock_create_main.return_value = {'name': 'main_view'}
        mock_create_nested.return_value = {'name': 'nested_view'}
        
        # Mock dimension and measure generators
        mock_dimension_gen = Mock()
        mock_measure_gen = Mock()
        
        array_models = [Mock()]
        
        result = generator.generate(
            model=sample_model,
            view_name="test_view",
            view_label="Test View",
            exclude_names=[],
            array_models=array_models,
            dimension_generator=mock_dimension_gen,
            measure_generator=mock_measure_gen,
        )
        
        # Should return list with main view and nested views
        assert isinstance(result, list)
        assert len(result) >= 1  # At least main view

    @patch('dbt2lookml.generators.view.LookmlViewGenerator._create_main_view')
    def test_generate_without_array_models(self, mock_create_main, cli_args, sample_model):
        """Test generate method without array models."""
        generator = LookmlViewGenerator(cli_args)
        
        mock_create_main.return_value = {'name': 'main_view'}
        
        # Mock dimension and measure generators
        mock_dimension_gen = Mock()
        mock_measure_gen = Mock()
        
        result = generator.generate(
            model=sample_model,
            view_name="test_view",
            view_label="Test View",
            exclude_names=[],
            array_models=[],
            dimension_generator=mock_dimension_gen,
            measure_generator=mock_measure_gen,
        )
        
        # Should return list with only main view
        assert isinstance(result, list)
        assert len(result) == 1

    def test_create_main_view_no_dimensions(self, cli_args, sample_model, mock_measure_generator):
        """Test _create_main_view when no dimensions are generated."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock dimension generator that returns None/empty
        mock_dimension_generator = Mock()
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (None, [])
        mock_dimension_generator.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [],
            'dimension_sets': []
        }
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            # Should still have basic view structure
            assert result['name'] == 'test_view'
            assert result['sql_table_name'] == '`project.dataset.test_table`'

    def test_create_main_view_with_empty_nested_dimensions(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view when nested dimensions is None but dimensions exist."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock dimensions but no nested dimensions
        mock_dimension_generator.lookml_dimensions_from_model.return_value = (
            [{"name": "id", "type": "string"}],
            None  # nested dimensions is None
        )
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert 'dimensions' in result
            assert len(result['dimensions']) == 1

    def test_create_main_view_case_insensitive_name(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_main_view converts view name to lowercase."""
        generator = LookmlViewGenerator(cli_args)
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            
            result = generator._create_main_view(
                model=sample_model,
                view_name="TEST_VIEW_UPPERCASE",
                view_label="Test View",
                exclude_names=[],
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
            )
            
            assert result['name'] == 'test_view_uppercase'

    def test_create_nested_view_with_complex_array_model(self, cli_args, sample_model, mock_dimension_generator, mock_measure_generator):
        """Test _create_nested_view with complex array model structure."""
        generator = LookmlViewGenerator(cli_args)
        
        array_model = DbtModelColumn(
            name="markings",
            lookml_name="markings",
            lookml_long_name="markings",
            data_type="ARRAY<STRUCT<marking STRUCT<code STRING, description STRING>>>",
            nested=False,
        )
        
        # Mock nested columns
        nested_columns = {
            "markings.marking.code": DbtModelColumn(
                name="markings.marking.code",
                lookml_name="code",
                lookml_long_name="markings__marking__code",
                data_type="STRING",
                nested=True,
            ),
            "markings.marking.description": DbtModelColumn(
                name="markings.marking.description", 
                lookml_name="description",
                lookml_long_name="markings__marking__description",
                data_type="STRING",
                nested=True,
            ),
        }
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.nested_view_columns = nested_columns
            
            result = generator._create_nested_view(
                model=sample_model,
                base_name="test_model",
                array_model=array_model,
                view_label="Test View",
                dimension_generator=mock_dimension_generator,
                measure_generator=mock_measure_generator,
                array_models=[],
            )
            
            assert result['name'] == 'test_model__markings'
            # Nested views don't have sql_table_name, they reference the main view
            assert 'name' in result

    def test_generate_integration(self, cli_args, sample_model):
        """Test full generate method integration."""
        generator = LookmlViewGenerator(cli_args)
        
        # Mock dimension and measure generators
        mock_dim_gen = Mock()
        mock_dim_gen.lookml_dimensions_from_model.return_value = (
            [{"name": "id", "type": "string"}], []
        )
        mock_dim_gen.lookml_dimension_groups_from_model.return_value = {
            'dimension_groups': [], 'dimension_sets': []
        }
        
        mock_measure_gen = Mock()
        mock_measure_gen.lookml_measures_from_model.return_value = []
        
        with patch('dbt2lookml.models.column_collections.ColumnCollections.from_model') as mock_collections:
            mock_collections.return_value.main_view_columns = sample_model.columns
            mock_collections.return_value.array_models = []
            
            result = generator.generate(
                model=sample_model,
                view_name="test_view",
                view_label="Test View",
                exclude_names=[],
                array_models=[],
                dimension_generator=mock_dim_gen,
                measure_generator=mock_measure_gen,
            )
            
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]['name'] == 'test_view'
