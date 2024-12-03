import pytest
from dbt2looker_bigquery.parser import DbtParser
from dbt2looker_bigquery.models import DbtModel, DbtCatalogNode

class TestDbtParser:
    @pytest.fixture
    def sample_manifest(self):
        return {
            "metadata": {
                "adapter_type": "bigquery"
            },
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
                            "meta": {"looker": {"hidden": False}}
                        }
                    },
                    "meta": {
                        "looker": {
                            "label": "Model 1"
                        }
                    },
                    "path": "models/test_model.sql"
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
                            "meta": {"looker": {"hidden": False}}
                        }
                    },
                    "meta": {
                        "looker": {
                            "label": "Model 2"
                        }
                    },
                    "path": "models/test_model.sql"
                }
            },
            "exposures": {
                "exposure.test.dashboard1": {
                    "resource_type": "exposure",
                    "name": "dashboard1",
                    "type": "dashboard",
                    "tags": ["analytics_dashboard"],
                    "depends_on": ["model.test.model1"],
                    "unique_id": "exposure.test.dashboard1",
                    "refs": [
                        {
                            "name": "model1",
                            "package": "test"
                        }
                    ]
                }
            }
        }

    @pytest.fixture
    def sample_catalog(self):
        return {
            "nodes": {
                "model.test.model1": {
                    "unique_id": "model.test.model1",
                    "metadata": {
                        "type": "table",
                        "schema": "test_schema",
                        "name": "model1"
                    },
                    "columns": {
                        "id": {
                            "name": "id",
                            "type": "INT64",
                            "data_type": "INT64",
                            "inner_types": ["INT64"],
                            "index": 1
                        }
                    }
                }
            }
        }

    @pytest.fixture
    def parser(self, sample_manifest, sample_catalog):
        return DbtParser(sample_manifest, sample_catalog)

    def test_parse_models_no_filter(self, parser):
        """Test parsing all models without any filters"""
        models = parser.parse_models()
        assert len(models) == 2
        assert {model.name for model in models} == {"model1", "model2"}

    def test_parse_models_with_tag(self, parser):
        """Test parsing models filtered by tag"""
        models = parser.parse_models(tag="analytics")
        assert len(models) == 1
        assert models[0].name == "model1"

    def test_parse_models_with_exposures(self, parser):
        """Test parsing models filtered by exposures"""
        models = parser.parse_models(exposures_only=True)
        assert len(models) == 1
        assert models[0].name == "model1"

    def test_parse_models_with_exposures_and_tag(self, parser):
        """Test parsing models filtered by both exposures and tag"""
        models = parser.parse_models(exposures_only=True, tag="analytics")
        assert len(models) == 1
        assert models[0].name == "model1"

        # Should return empty when tag doesn't match exposed model
        models = parser.parse_models(exposures_only=True, tag="reporting")
        assert len(models) == 0

    def test_parse_models_with_select_model(self, parser):
        """Test parsing specific model by name"""
        models = parser.parse_models(select_model="model2")
        assert len(models) == 1
        assert models[0].name == "model2"

    def test_get_column_type_from_catalog(self, parser):
        """Test retrieving column type from catalog"""
        data_type, inner_types = parser._get_catalog_column_info("model.test.model1", "id")
        assert data_type == "INT64"
        assert inner_types == ["INT64"]  # INT64 column has INT64 inner type in test fixture

        # Test non-existent model/column
        data_type, inner_types = parser._get_catalog_column_info("non.existent.model", "id")
        assert data_type is None
        assert inner_types == []  # Empty list for non-existent columns

    def test_tags_match(self, parser):
        """Test tag matching functionality"""
        model_with_tags = DbtModel(
            resource_type="model",
            name="test",
            tags=["tag1", "tag2"],
            unique_id="model.test",
            relation_name="test",
            schema="test_schema",
            description="Test model",
            columns={},
            meta={"looker": {}},
            path="models/test.sql"
        )
        assert parser._tags_match("tag1", model_with_tags) is True
        assert parser._tags_match("tag3", model_with_tags) is False

        # Test model without tags
        model_without_tags = DbtModel(
            resource_type="model",
            name="test",
            unique_id="model.test",
            relation_name="test",
            schema="test_schema",
            description="Test model",
            columns={},
            meta={"looker": {}},
            path="models/test.sql",
            tags=[]
        )
        assert parser._tags_match("tag1", model_without_tags) is False
