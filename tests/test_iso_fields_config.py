"""Tests for ISO fields configuration functionality."""

from unittest.mock import Mock

from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.models.dbt import DbtModelColumn


class TestIsoFieldsConfiguration:
    """Test suite for ISO fields configuration functionality."""

    def test_default_iso_fields_disabled(self):
        """Test that ISO fields are disabled by default."""
        args = Mock()
        args.include_iso_fields = False
        args.timeframes = {}
        generator = LookmlDimensionGenerator(args)
        assert generator._include_iso_fields is False

    def test_iso_fields_enabled_via_cli(self):
        """Test that ISO fields can be enabled via CLI argument."""
        args = Mock()
        args.include_iso_fields = True
        args.timeframes = {}
        generator = LookmlDimensionGenerator(args)
        assert generator._include_iso_fields is True

    def test_iso_fields_generation_with_enabled(self):
        """Test that ISO fields are generated when enabled."""
        args = Mock()
        args.include_iso_fields = True
        args.timeframes = {}
        generator = LookmlDimensionGenerator(args)
        # Create a mock date column
        column = Mock(spec=DbtModelColumn)
        column.name = "test_date"
        column.data_type = "DATE"
        column.description = "Test date column"
        column.meta = Mock()
        column.meta.looker = Mock()
        column.meta.looker.group_label = None
        column.meta.looker.label = None
        column.lookml_name = "test_date"
        # Create mock model
        model = Mock()
        dimension_group, dimension_group_set, dimensions = generator.lookml_dimension_group(
            column, "date", True, model
        )
        # Should have ISO fields
        iso_field_names = [d["name"] for d in dimensions]
        assert "test_date_iso_year" in iso_field_names
        assert "test_date_iso_week_of_year" in iso_field_names
        # Should be in dimension group set
        assert "test_date_iso_year" in dimension_group_set["fields"]
        assert "test_date_iso_week_of_year" in dimension_group_set["fields"]

    def test_iso_fields_generation_with_disabled(self):
        """Test that ISO fields are not generated when disabled."""
        args = Mock()
        args.include_iso_fields = False
        args.timeframes = {}
        generator = LookmlDimensionGenerator(args)
        # Create a mock date column
        column = Mock(spec=DbtModelColumn)
        column.name = "test_date"
        column.data_type = "DATE"
        column.description = "Test date column"
        column.meta = Mock()
        column.meta.looker = Mock()
        column.meta.looker.group_label = None
        column.meta.looker.label = None
        column.lookml_name = "test_date"
        dimension_group, dimension_group_set, dimensions = generator.lookml_dimension_group(
            column, "test_table", False, model=None
        )
        # Should not have ISO fields - when disabled, dimensions should be empty
        assert dimensions is None or dimensions == []

    def test_non_date_columns_not_affected(self):
        """Test that non-date columns are not affected by ISO fields configuration."""
        args = Mock()
        args.include_iso_fields = False
        args.timeframes = {}
        generator = LookmlDimensionGenerator(args)
        # Create a mock string column
        column = Mock(spec=DbtModelColumn)
        column.name = "test_string"
        column.data_type = "STRING"
        column.description = "Test string column"
        column.meta = Mock()
        column.meta.looker = Mock()
        column.meta.looker.group_label = None
        column.meta.looker.label = None
        column.lookml_name = "test_string"
        # Create mock model
        model = Mock()
        result = generator.lookml_dimension_group(column, "date", True, model)
        # For non-date columns, should return empty dimensions
        dimension_group, dimension_group_set, dimensions = result
        assert dimensions == []
        # Should not have any ISO fields for non-date columns
        assert len(result[2]) == 0
