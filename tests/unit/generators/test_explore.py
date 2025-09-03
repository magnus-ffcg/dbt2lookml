"""Test LookML Explore Generator implementations."""

from argparse import Namespace
from unittest.mock import Mock

import pytest

from dbt2lookml.generators.explore import LookmlExploreGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelMeta
from dbt2lookml.models.looker import DbtMetaLooker


@pytest.fixture
def cli_args():
    """Create CLI args fixture."""
    return Namespace(
        use_table_name=False,
        include_models=[],
        exclude_models=[],
        target_dir='output',
    )


@pytest.fixture
def sample_model():
    """Create a sample DbtModel for testing."""
    return DbtModel(
        unique_id='model.test.sample_model',
        name='sample_model',
        relation_name='sample_table',
        schema='test_schema',
        description='Test model',
        tags=[],
        path='models/sample_model.sql',
        columns={},
        meta=DbtModelMeta(looker=DbtMetaLooker()),
    )


class TestLookmlExploreGenerator:
    """Test cases for LookmlExploreGenerator class."""

    def test_init(self, cli_args):
        """Test LookmlExploreGenerator initialization."""
        generator = LookmlExploreGenerator(cli_args)
        assert generator._cli_args == cli_args

    def test_generate_basic_explore(self, cli_args, sample_model):
        """Test basic explore generation."""
        generator = LookmlExploreGenerator(cli_args)

        view_name = 'sample_model'
        view_label = 'Sample Model'
        array_models = []

        explore = generator.generate(sample_model, view_name, view_label, array_models)

        assert explore is not None
        assert 'name' in explore
        assert explore['name'] == 'sample_model'
