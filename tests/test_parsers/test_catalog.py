"""Tests for the catalog parser module."""
import pytest
from dbt2lookml.models.dbt import DbtModel, DbtCatalog, DbtModelColumnMeta
from dbt2lookml.parsers.catalog import CatalogParser

class TestCatalogParser:
    @pytest.fixture
    def sample_catalog(self):
        return DbtCatalog(**{
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
        })

    @pytest.fixture
    def parser(self, sample_catalog):
        return CatalogParser(sample_catalog)

    def test_get_catalog_column_info(self, parser):
        """Test retrieving column type from catalog."""
        data_type, inner_types = parser._get_catalog_column_info("model.test.model1", "id")
        assert data_type == "INT64"
        assert inner_types == ["INT64"]

        # Test non-existent model/column
        data_type, inner_types = parser._get_catalog_column_info("non.existent.model", "id")
        assert data_type is None
        assert inner_types == []

    def test_create_missing_array_column(self, parser):
        """Test creating a missing array column."""
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

    def test_process_model(self, parser):
        """Test processing a model with catalog information."""
        model = DbtModel(
            resource_type="model",
            name="model1",
            unique_id="model.test.model1",
            relation_name="model1",
            schema="test_schema",
            description="Test model",
            columns={
                "id": {
                    "name": "id",
                    "description": "Primary key",
                    "data_type": None,  # This will be filled from catalog
                    "meta": {"looker": {"hidden": False}},
                }
            },
            meta={"looker": {}},
            path="models/test.sql",
            tags=[]  # Add empty tags list
        )

        processed_model = parser.process_model(model)
        assert processed_model is not None
        assert processed_model.columns["id"].data_type == "INT64"
        assert processed_model.columns["id"].inner_types == ["INT64"]
