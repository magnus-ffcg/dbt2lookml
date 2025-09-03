"""Test catalog parser dimension naming regression fix."""

import pytest

from dbt2lookml.models.dbt import DbtCatalog, DbtModelColumnMeta
from dbt2lookml.parsers.catalog import CatalogParser


class TestCatalogParserDimensionNaming:
    """Test catalog parser dimension naming fixes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = CatalogParser(DbtCatalog(nodes={}))

    def test_create_missing_array_column_camel_case_conversion(self):
        """Test that array columns convert CamelCase to snake_case for lookml_name."""
        column = self.parser._create_missing_array_column("CountryOfOrigin", "ARRAY<STRING>", ["STRING"])

        assert column.name == "countryoforigin"  # Model validation converts to lowercase
        assert column.lookml_name == "country_of_origin"  # Our fix applies camel_to_snake
        assert column.original_name == "CountryOfOrigin"

    def test_create_missing_nested_column_camel_case_conversion(self):
        """Test that nested columns convert CamelCase to snake_case for lookml_name."""
        column = self.parser._create_missing_nested_column(
            "items.CountryOfOrigin", "STRING", "Country info", "items.CountryOfOrigin"
        )

        assert column.name == "items.countryoforigin"  # Model validation converts to lowercase
        assert column.lookml_name == "country_of_origin"  # Only last part converted
        assert column.original_name == "items.CountryOfOrigin"

    def test_create_missing_array_column_already_snake_case(self):
        """Test that already snake_case columns remain unchanged."""
        column = self.parser._create_missing_array_column("country_of_origin", "ARRAY<STRING>", ["STRING"])

        assert column.name == "country_of_origin"
        assert column.lookml_name == "country_of_origin"
        assert column.original_name == "country_of_origin"

    def test_create_missing_nested_column_already_snake_case(self):
        """Test that already snake_case nested columns remain unchanged."""
        column = self.parser._create_missing_nested_column(
            "items.country_of_origin", "STRING", "Country info", "items.CountryOfOrigin"
        )

        assert column.name == "items.country_of_origin"
        assert column.lookml_name == "country_of_origin"
        assert column.original_name == "items.CountryOfOrigin"

    def test_create_missing_array_column_lowercase_no_conversion(self):
        """Test that lowercase columns without underscores remain unchanged."""
        column = self.parser._create_missing_array_column("countryoforigin", "ARRAY<STRING>", ["STRING"])

        assert column.name == "countryoforigin"
        assert column.lookml_name == "countryoforigin"  # No conversion possible
        assert column.original_name == "countryoforigin"

    def test_create_missing_nested_column_complex_camel_case(self):
        """Test complex CamelCase conversion in nested columns."""
        column = self.parser._create_missing_nested_column(
            "ItemEBO.PackStructure.Item.GlobalAttributes.PlaceOfActivityModule.PlaceOfProductActivity.CountryOfOrigin.CodeValue",
            "STRING",
            "Code value",
            "ItemEBO.PackStructure.Item.GlobalAttributes.PlaceOfActivityModule.PlaceOfProductActivity.CountryOfOrigin.CodeValue",
        )

        assert column.lookml_name == "code_value"  # Only last part converted
        assert (
            column.original_name
            == "ItemEBO.PackStructure.Item.GlobalAttributes.PlaceOfActivityModule.PlaceOfProductActivity.CountryOfOrigin.CodeValue"
        )
