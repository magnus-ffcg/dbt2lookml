"""Tests for the base parser module."""
import pytest
import argparse
from dbt2lookml.models.dbt import DbtModel, DbtCatalog, DbtManifest
from dbt2lookml.parsers.base import DbtParser

class TestDbtParser:
    @pytest.fixture
    def sample_manifest(self):
        return {
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
                }
            },
            "exposures": {
                "exposure.test.dashboard1": {
                    "resource_type": "exposure",
                    "name": "dashboard1",
                    "type": "dashboard",
                    "tags": ["analytics_dashboard"],
                    "depends_on": {
                        "nodes": [
                            "model.test.model1"
                        ],
                    },
                    "unique_id": "exposure.test.dashboard1",
                    "refs": [{"name": "model1", "package": "test"}],
                }
            }
        }

    @pytest.fixture
    def sample_catalog(self):
        return {
            "nodes": {
                "model.test.model1": {
                    "unique_id": "model.test.model1",
                    "metadata": {"type": "table", "schema": "test_schema", "name": "model1"},
                    "columns": {
                        "id": {
                            "name": "id",
                            "type": "INT64",
                            "data_type": "INT64",
                            "inner_types": ["INT64"],
                            "index": 1,
                        }
                    },
                }
            }
        }

    #@pytest.fixture
    #def parser(self, sample_manifest, sample_catalog):
    #    return DbtParser(sample_manifest, sample_catalog)

    def test_get_models_no_filter(self, sample_manifest, sample_catalog):
        """Test parsing all models without any filters."""
        self.assert_parser_output(None, None, False, sample_manifest, sample_catalog)

    def test_get_models_with_tag(self, sample_manifest, sample_catalog):
        """Test parsing models filtered by tag."""
        self.assert_parser_output(
            None, "analytics", False, sample_manifest, sample_catalog
        )

    def test_get_models_with_exposures(self, sample_manifest, sample_catalog):
        """Test parsing models filtered by exposures."""
        self.assert_parser_output(None, None, True, sample_manifest, sample_catalog)

    def test_get_models_with_select(self, sample_manifest, sample_catalog):
        """Test parsing specific model by name."""
        self.assert_parser_output(
            "model1", None, False, sample_manifest, sample_catalog
        )

    # TODO Rename this here and in `test_get_models_no_filter`, `test_get_models_with_tag`, `test_get_models_with_exposures` and `test_get_models_with_select`
    def assert_parser_output(self, select_model, tag, build_explore, sample_manifest, sample_catalog):
        args = argparse.Namespace(
            select_model=select_model,
            tag=tag,
            exposures_tag=None,
            build_explore=build_explore,
        )
        parser = DbtParser(args, sample_manifest, sample_catalog)
        models = parser.get_models()
        assert len(models) == 1
        assert models[0].name == "model1"
