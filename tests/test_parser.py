import pytest
import logging
from dbt2lookml.parsers import DbtParser
from dbt2lookml.models import DbtModel, DbtCatalogNode, DbtCatalogNodeColumn, DbtModelColumnMeta, DbtModelColumn, DbtMetaMeasure
from dbt2lookml.enums import LookerMeasureType

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

    def test_filter_nodes_by_type(self, parser):
        """Test filtering nodes by resource type"""
        models = parser._filter_nodes_by_type(parser._manifest.nodes, "model")
        assert len(models) == 2
        assert all(model.resource_type == "model" for model in models)

        exposures = parser._filter_nodes_by_type(parser._manifest.exposures, "exposure")
        assert len(exposures) == 1
        assert all(exp.resource_type == "exposure" for exp in exposures)

    def test_filter_models(self, parser):
        """Test filtering models with various criteria"""
        all_models = parser._filter_nodes_by_type(parser._manifest.nodes, "model")
        
        # Test filtering by select_model
        filtered = parser._filter_models(all_models, select_model="model1")
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

        # Test filtering by tag
        filtered = parser._filter_models(all_models, tag="analytics")
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

        # Test filtering by exposed_names
        filtered = parser._filter_models(all_models, exposed_names=["model1"])
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

        # Test multiple filters
        filtered = parser._filter_models(all_models, tag="analytics", exposed_names=["model1"])
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

    def test_get_models_no_filter(self, parser):
        """Test parsing all models without any filters"""
        models = parser.get_models()
        assert len(models) == 2
        assert {model.name for model in models} == {"model1", "model2"}

    def test_get_models_with_tag(self, parser):
        """Test parsing models filtered by tag"""
        models = parser.get_models(tag="analytics")
        assert len(models) == 1
        assert models[0].name == "model1"

    def test_get_models_with_exposures(self, parser):
        """Test parsing models filtered by exposures"""
        models = parser.get_models(exposures_only=True)
        assert len(models) == 1
        assert models[0].name == "model1"

    def test_get_models_with_exposures_and_tag(self, parser):
        """Test parsing models filtered by both exposures and tag"""
        models = parser.get_models(exposures_only=True, tag="analytics")
        assert len(models) == 1
        assert models[0].name == "model1"

        # Should return empty when tag doesn't match exposed model
        models = parser.get_models(exposures_only=True, tag="reporting")
        assert len(models) == 0

    def test_get_models_with_select_model(self, parser):
        """Test parsing specific model by name"""
        models = parser.get_models(select_model="model2")
        assert len(models) == 1
        assert models[0].name == "model2"

    def test_get_catalog_column_info(self, parser):
        """Test retrieving column type from catalog"""
        data_type, inner_types = parser._get_catalog_column_info("model.test.model1", "id")
        assert data_type == "INT64"
        assert inner_types == ["INT64"]

        # Test non-existent model/column
        data_type, inner_types = parser._get_catalog_column_info("non.existent.model", "id")
        assert data_type is None
        assert inner_types == []

    def test_update_column_with_types(self, parser, sample_manifest):
        """Test updating column with type information"""
        model = DbtModel(**sample_manifest["nodes"]["model.test.model1"])
        column = list(model.columns.values())[0]
        
        updated_column = parser._update_column_with_types(column, model.unique_id)
        assert updated_column.data_type == "INT64"
        assert updated_column.inner_types == ["INT64"]

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

    def test_process_typed_models(self, parser):
        """Test processing models with type information"""
        models = parser.get_models(tag="analytics")
        typed_models = parser._process_typed_models(models)
        
        assert len(typed_models) == 1
        model = typed_models[0]
        assert model.name == "model1"
        assert list(model.columns.values())[0].data_type == "INT64"


    def test_create_missing_array_column(self, parser):
        """Test creating a missing array column"""
        column = parser._create_missing_array_column(
            column_name="test_array",
            data_type="ARRAY<STRING>",
            inner_types=["STRING"]
        )
        
        assert column.name == "test_array"
        assert column.data_type == "ARRAY<STRING>"
        assert column.inner_types == ["STRING"]
        assert column.description == "missing column from manifest.json, generated from catalog.json"
        assert isinstance(column.meta, DbtModelColumnMeta)

    def test_log_model_stats(self, parser, caplog):
        """Test logging model statistics"""
        caplog.set_level(logging.DEBUG)

        # Create test models
        model1 = DbtModel(
            resource_type="model",
            name="test1",
            unique_id="model.test.test1",
            relation_name="test1",
            schema="test_schema",
            lookml_long_name="test1",
            lookml_name="test1",
            description="Test model 1",
            tags=[],
            columns={
                "col1": DbtModelColumn(
                    name="col1",
                    data_type='STRING',
                    meta=DbtModelColumnMeta(looker_measures=[
                        DbtMetaMeasure(type=LookerMeasureType.COUNT),
                        DbtMetaMeasure(type=LookerMeasureType.SUM)
                    ])
                )
            },
            meta={"looker": {}},
            path="models/test.sql"
        )

        # Test logging
        parser._log_model_stats([model1])

        # Check log messages
        assert "Found manifest entries for 1 models" in caplog.text
        assert "Model test1 has 1 columns with 2 measures" in caplog.text

    def test_log_typed_model_stats(self, parser, caplog):
        """Test logging typed model statistics"""
        caplog.set_level(logging.DEBUG)

        # Create test models
        original_model = DbtModel(
            resource_type="model",
            name="test1",
            unique_id="model.test.test1",
            relation_name="test1",
            schema="test_schema",
            lookml_long_name="test1",
            lookml_name="test1",
            description="Test model 1",
            tags=[],
            columns={
                "col1": DbtModelColumn(
                    name="col1",
                    data_type='STRING',
                    meta=DbtModelColumnMeta()
                )
            },
            meta={"looker": {}},
            path="models/test.sql"
        )

        typed_model = DbtModel(
            resource_type="model",
            name="test1",
            unique_id="model.test.test1",
            relation_name="test1",
            schema="test_schema",
            lookml_long_name="test1",
            lookml_name="test1",
            description="Test model 1",
            tags=[],
            columns={
                "col1": DbtModelColumn(
                    name="col1",
                    data_type='STRING',
                    meta=DbtModelColumnMeta()
                ),
                "col2": DbtModelColumn(
                    name="col2",
                    data_type='INT64',
                    meta=DbtModelColumnMeta()
                )
            },
            meta={"looker": {}},
            path="models/test.sql"
        )

        # Test logging
        parser._log_typed_model_stats([original_model], [typed_model])

        # Check log messages
        assert "Found catalog entries for 1 models" in caplog.text
        assert "Catalog entries missing for 0 models" in caplog.text
