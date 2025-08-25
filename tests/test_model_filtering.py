"""Tests for model filtering functionality including include/exclude models."""

from typing import List
from unittest.mock import Mock

from dbt2lookml.models.dbt import DbtModel
from dbt2lookml.parsers.model import ModelParser


class TestModelFiltering:
    """Test suite for model filtering functionality."""

    def create_mock_model(self, name: str, tags: List[str] = None) -> DbtModel:
        """Create a mock DbtModel for testing."""
        model = Mock(spec=DbtModel)
        model.name = name
        model.tags = tags or []
        model.resource_type = 'model'
        return model

    def test_filter_models_no_filters(self):
        """Test that all models are returned when no filters are applied."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
        ]
        # Act
        result = parser.filter_models(models)
        # Assert
        assert len(result) == 3
        assert [m.name for m in result] == ['model1', 'model2', 'model3']

    def test_filter_models_include_models_single(self):
        """Test filtering with a single include model."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
        ]
        # Act
        result = parser.filter_models(models, include_models=['model2'])
        # Assert
        assert len(result) == 1
        assert result[0].name == 'model2'

    def test_filter_models_include_models_multiple(self):
        """Test filtering with multiple include models."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
            self.create_mock_model('model4'),
        ]
        # Act
        result = parser.filter_models(models, include_models=['model1', 'model3'])
        # Assert
        assert len(result) == 2
        assert [m.name for m in result] == ['model1', 'model3']

    def test_filter_models_exclude_models_single(self):
        """Test filtering with a single exclude model."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
        ]
        # Act
        result = parser.filter_models(models, exclude_models=['model2'])
        # Assert
        assert len(result) == 2
        assert [m.name for m in result] == ['model1', 'model3']

    def test_filter_models_exclude_models_multiple(self):
        """Test filtering with multiple exclude models."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
            self.create_mock_model('model4'),
        ]
        # Act
        result = parser.filter_models(models, exclude_models=['model1', 'model3'])
        # Assert
        assert len(result) == 2
        assert [m.name for m in result] == ['model2', 'model4']

    def test_filter_models_include_and_exclude_combined(self):
        """Test filtering with both include and exclude models."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
            self.create_mock_model('model4'),
            self.create_mock_model('model5'),
        ]
        # Act - include models 1,2,3,4 but exclude 2,4
        result = parser.filter_models(
            models,
            include_models=['model1', 'model2', 'model3', 'model4'],
            exclude_models=['model2', 'model4'],
        )
        # Assert - should only have model1 and model3
        assert len(result) == 2
        assert [m.name for m in result] == ['model1', 'model3']

    def test_filter_models_include_overrides_select(self):
        """Test that include_models works with select_model (select takes precedence)."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
            self.create_mock_model('model3'),
        ]
        # Act - select_model should take precedence over include_models
        result = parser.filter_models(
            models, select_model='model2', include_models=['model1', 'model3']
        )
        # Assert - only model2 should be returned due to select_model precedence
        assert len(result) == 1
        assert result[0].name == 'model2'

    def test_filter_models_with_tag_filter(self):
        """Test that include/exclude works with tag filtering."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1', ['tag1']),
            self.create_mock_model('model2', ['tag2']),
            self.create_mock_model('model3', ['tag1']),
            self.create_mock_model('model4', ['tag2']),
        ]
        # Act - filter by tag first, then include/exclude
        result = parser.filter_models(
            models, tag='tag1', include_models=['model1', 'model3'], exclude_models=['model3']
        )
        # Assert - should have model1 (tag1 + included but not excluded)
        assert len(result) == 1
        assert result[0].name == 'model1'

    def test_filter_models_empty_include_exclude(self):
        """Test that empty include/exclude lists work correctly."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
        ]
        # Act
        result = parser.filter_models(models, include_models=[], exclude_models=[])
        # Assert - all models should be returned
        assert len(result) == 2

    def test_filter_models_nonexistent_names(self):
        """Test filtering with non-existent model names."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('model1'),
            self.create_mock_model('model2'),
        ]
        # Act
        result = parser.filter_models(
            models, include_models=['nonexistent'], exclude_models=['also_nonexistent']
        )
        # Assert - include should return empty, exclude should return all
        assert len(result) == 0

    def test_filter_models_case_sensitive(self):
        """Test that model name filtering is case-sensitive."""
        # Arrange
        parser = ModelParser(Mock())
        models = [
            self.create_mock_model('Model1'),
            self.create_mock_model('model1'),
        ]
        # Act
        result = parser.filter_models(models, include_models=['model1'])
        # Assert - only exact match should be returned
        assert len(result) == 1
        assert result[0].name == 'model1'


class TestDbtParserIntegration:
    """Integration tests for DbtParser with model filtering."""

    def test_dbt_parser_passes_include_exclude_args(self):
        """Test that DbtParser correctly passes include/exclude arguments to ModelParser."""
        # This is more of an integration test - we'll test the actual CLI integration
        # in the CLI tests
        pass
