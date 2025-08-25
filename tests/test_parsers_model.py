"""Tests for the model parser module."""

import pytest

from dbt2lookml.models.dbt import DbtManifest, DbtModel
from dbt2lookml.parsers.model import ModelParser


class TestModelParser:
    @pytest.fixture
    def sample_manifest(self):
        return DbtManifest(
            **{
                "metadata": {"adapter_type": "bigquery"},
                "nodes": {
                    "model.test.model1": {
                        "resource_type": "model",
                        "relation_name": "model1",
                        "schema": "test_schema",
                        "name": "model1",
                        "unique_id": "model.test.model1",
                        "tags": ["analytics"],
                        "description": "Test model 1",
                        "columns": {
                            "id": {
                                "name": "id",
                                "description": "Primary key",
                                "data_type": "INT64",
                                "meta": {"looker": {"hidden": False}},
                            }
                        },
                        "meta": {"looker": {"label": "Model 1"}},
                        "path": "models/test_model.sql",
                    },
                    "model.test.model2": {
                        "resource_type": "model",
                        "relation_name": "model2",
                        "schema": "test_schema",
                        "name": "model2",
                        "unique_id": "model.test.model2",
                        "tags": ["reporting"],
                        "description": "Test model 2",
                        "columns": {
                            "id": {
                                "name": "id",
                                "description": "Primary key",
                                "data_type": "INT64",
                                "meta": {"looker": {"hidden": False}},
                            }
                        },
                        "meta": {"looker": {"label": "Model 2"}},
                        "path": "models/test_model.sql",
                    },
                },
                "exposures": {},
            }
        )

    @pytest.fixture
    def parser(self, sample_manifest):
        return ModelParser(sample_manifest)

    def test_get_all_models(self, parser):
        """Test getting all models from manifest."""
        models = parser.get_all_models()
        assert len(models) == 2
        assert all(model.resource_type == "model" for model in models)
        assert {model.name for model in models} == {"model1", "model2"}

    def test_filter_models(self, parser):
        """Test filtering models with various criteria."""
        all_models = parser.get_all_models()
        # Test filtering by select_model
        filtered = parser.filter_models(all_models, select_model="model1")
        assert len(filtered) == 1
        assert filtered[0].name == "model1"
        # Test filtering by tag
        filtered = parser.filter_models(all_models, tag="analytics")
        assert len(filtered) == 1
        assert filtered[0].name == "model1"
        # Test filtering by exposed_names
        filtered = parser.filter_models(all_models, exposed_names=["model1"])
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

    def test_tags_match(self, parser):
        """Test tag matching functionality."""
        model = DbtModel(
            resource_type="model",
            name="test",
            tags=["tag1", "tag2"],
            unique_id="model.test",
            relation_name="test",
            schema="test_schema",
            description="Test model",
            columns={},
            meta={"looker": {}},
            path="models/test.sql",
        )
        assert parser._tags_match("tag1", model) is True
        assert parser._tags_match("tag3", model) is False
