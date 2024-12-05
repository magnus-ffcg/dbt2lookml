import pytest
from dbt2lookml.models import (
    DbtModel, DbtModelColumn, DbtModelMeta, DbtMetaLooker,
    DbtCatalogNode, DbtCatalogNodeColumn, DbtModelColumnMeta
)

class TestDbtModels:
    @pytest.fixture
    def sample_model_data(self):
        return {
            "resource_type": "model",
            "relation_name": "my_model",
            "schema": "my_schema",
            "name": "test_model",
            "description": "A test model",
            "columns": {
                "id": {
                    "name": "id",
                    "description": "Primary key",
                    "data_type": "INT64",
                    "meta": {
                        "looker": {
                            "hidden": False,
                            "label": "ID"
                        }
                    }
                }
            },
            "unique_id": "test_model.id",
            "tags": ["test"],
            "meta": {
                "looker": {
                    "label": "Test Model",
                    "hidden": False
                }
            },
            "path": "models/test_model.sql"
        }

    def test_dbt_model_creation(self, sample_model_data):
        """Test creating a DbtModel instance"""
        model = DbtModel(**sample_model_data)
        assert model.name == "test_model"
        assert model.resource_type == "model"
        assert model.db_schema == "my_schema"
        assert isinstance(model.meta, DbtModelMeta)
        assert isinstance(model.meta.looker, DbtMetaLooker)
        assert len(model.columns) == 1
        assert isinstance(model.columns["id"], DbtModelColumn)

    def test_dbt_model_column_validation(self):
        """Test DbtModelColumn validation"""
        column = DbtModelColumn(
            name="test_col",
            lookml_name="test_col",
            lookml_long_name="test_col",
            description="Test column",
            data_type="STRING",
            meta=DbtModelColumnMeta()
        )
        assert column.name == "test_col"
        assert column.data_type == "STRING"
        assert not column.nested
        assert not column.is_primary_key

    @pytest.mark.parametrize("value,expected", [
        (True, True),
        (False, False),
    ])
    def test_yes_no_validator(self, value, expected):
        """Test yes/no validation in meta fields"""
        meta = DbtMetaLooker(hidden=value)
        assert meta.hidden == expected

class TestDbtCatalog:
    @pytest.fixture
    def sample_catalog_node(self):
        return {
            "metadata": {
                "type": "table",
                "schema": "test_schema",
                "name": "test_table",
            },
            "columns": {
                "id": {
                    "type": "INT64",
                    "index": 1,
                    "name": "id"
                }
            }
        }

    def test_catalog_node_creation(self, sample_catalog_node):
        """Test creating a DbtCatalogNode instance"""
        node = DbtCatalogNode(**sample_catalog_node)
        assert node.metadata.type == "table"
        assert node.metadata.db_schema == "test_schema"
        assert isinstance(node.columns["id"], DbtCatalogNodeColumn)
        assert node.columns["id"].type == "INT64"
