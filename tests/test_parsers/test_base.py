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

    @pytest.fixture
    def parser(self, sample_manifest, sample_catalog):
        return DbtParser(sample_manifest, sample_catalog)

    def test_get_models_no_filter(self, parser):
        """Test parsing all models without any filters."""
        self._extracted_from_test_get_models_with_select_3(None, None, False, parser)

    def test_get_models_with_tag(self, parser):
        """Test parsing models filtered by tag."""
        self._extracted_from_test_get_models_with_select_3(
            None, "analytics", False, parser
        )

    def test_get_models_with_exposures(self, parser):
        """Test parsing models filtered by exposures."""
        self._extracted_from_test_get_models_with_select_3(None, None, True, parser)

    def test_get_models_with_select(self, parser):
        """Test parsing specific model by name."""
        self._extracted_from_test_get_models_with_select_3(
            "model1", None, False, parser
        )

    # TODO Rename this here and in `test_get_models_no_filter`, `test_get_models_with_tag`, `test_get_models_with_exposures` and `test_get_models_with_select`
    def _extracted_from_test_get_models_with_select_3(self, select_model, tag, build_explore, parser):
        args = argparse.Namespace(
            select_model=select_model,
            tag=tag,
            exposures_tag=None,
            build_explore=build_explore,
        )
        models = parser.get_models(args)
        assert len(models) == 1
        assert models[0].name == "model1"
