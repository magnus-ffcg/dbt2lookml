"""Test dimension conflict renaming functionality."""

from unittest.mock import Mock

import pytest

from dbt2lookml.generators.dimension import LookmlDimensionGenerator


class TestDimensionConflictRename:
    """Test dimension conflict renaming functionality."""

    @pytest.fixture
    def generator(self):
        """Create a dimension generator for testing."""
        args = Mock()
        args.use_table_name = False
        args.include_iso_fields = False
        args.generate_locale = False
        args.timeframes = {}
        return LookmlDimensionGenerator(args)

    def test_comment_conflicting_dimensions_rename_with_suffix(self, generator):
        """Test that conflicting dimensions are renamed with '_conflict' suffix."""
        # Test dimensions that will conflict with dimension groups
        dimensions = [
            {
                "name": "created_date",
                "type": "string",
                "sql": "${TABLE}.created_date",
                "description": "Original created date field",
            },
            {
                "name": "created",  # Conflicts with dimension group base name
                "type": "string",
                "sql": "${TABLE}.created",
                "description": "Conflicting field",
            },
            {"name": "normal_field", "type": "string", "sql": "${TABLE}.normal_field", "description": "Non-conflicting field"},
        ]

        # Dimension groups that cause conflicts
        dimension_groups = [
            {
                "name": "created",
                "type": "time",
                "sql": "${TABLE}.created_timestamp",
                "timeframes": ["date", "week", "month", "year"],
            }
        ]

        # Test the conflict resolution
        processed_dims = generator._comment_conflicting_dimensions(dimensions, dimension_groups, "test_model")

        # Verify results
        assert len(processed_dims) == 3, "All dimensions should be preserved (renamed, not removed)"

        # Check that conflicting dimensions were renamed
        renamed_dims = [dim for dim in processed_dims if dim['name'].endswith('_conflict')]
        assert len(renamed_dims) == 2, "Two dimensions should be renamed with '_conflict' suffix"

        # Verify specific renames
        created_date_conflict = next((dim for dim in processed_dims if dim['name'] == 'created_date_conflict'), None)
        assert created_date_conflict is not None, "created_date should be renamed to created_date_conflict"
        assert (
            created_date_conflict['hidden']
            == "yes # Renamed from 'created_date' due to conflict with dimension group with same name"
        )

        created_conflict = next((dim for dim in processed_dims if dim['name'] == 'created_conflict'), None)
        assert created_conflict is not None, "created should be renamed to created_conflict"
        assert created_conflict['hidden'] == "yes # Renamed from 'created' due to conflict with dimension group with same name"

        # Verify non-conflicting dimension is unchanged
        normal_field = next((dim for dim in processed_dims if dim['name'] == 'normal_field'), None)
        assert normal_field is not None, "Non-conflicting dimension should remain unchanged"
        assert normal_field['description'] == "Non-conflicting field", "Description should be unchanged"
        assert 'hidden' not in normal_field, "Non-conflicting dimension should not be hidden"

    def test_comment_conflicting_dimensions_with_comment_and_hidden(self, generator):
        """Test that conflicting dimensions get comment and hidden attributes."""
        dimensions = [
            {
                "name": "created_date",
                "type": "string",
                "sql": "${TABLE}.created_date",
                "description": "Important business field for tracking creation",
            }
        ]

        dimension_groups = [
            {"name": "created", "type": "time", "sql": "${TABLE}.created_timestamp", "timeframes": ["date", "week", "month"]}
        ]

        processed_dims = generator._comment_conflicting_dimensions(dimensions, dimension_groups, "test_model")

        renamed_dim = processed_dims[0]
        assert renamed_dim['name'] == 'created_date_conflict'
        assert (
            renamed_dim['description'] == "Important business field for tracking creation"
        ), "Original description should be preserved"
        assert renamed_dim['hidden'] == "yes # Renamed from 'created_date' due to conflict with dimension group with same name"

    def test_comment_conflicting_dimensions_no_description(self, generator):
        """Test handling of dimensions without existing descriptions."""
        dimensions = [
            {
                "name": "created",
                "type": "string",
                "sql": "${TABLE}.created",
                # No description field
            }
        ]

        dimension_groups = [
            {"name": "created", "type": "time", "sql": "${TABLE}.created_timestamp", "timeframes": ["date", "week"]}
        ]

        processed_dims = generator._comment_conflicting_dimensions(dimensions, dimension_groups, "test_model")

        renamed_dim = processed_dims[0]
        assert renamed_dim['name'] == 'created_conflict'
        assert 'description' not in renamed_dim, "No description should be added when original had none"
        assert renamed_dim['hidden'] == "yes # Renamed from 'created' due to conflict with dimension group with same name"

    def test_comment_conflicting_dimensions_timeframe_conflicts(self, generator):
        """Test conflicts with specific timeframes generated by dimension groups."""
        dimensions = [
            {
                "name": "created_date",  # Will conflict with created_date timeframe
                "type": "string",
                "sql": "${TABLE}.created_date",
                "description": "Date field",
            },
            {
                "name": "created_week",  # Will conflict with created_week timeframe
                "type": "string",
                "sql": "${TABLE}.created_week",
                "description": "Week field",
            },
        ]

        dimension_groups = [
            {"name": "created", "type": "time", "sql": "${TABLE}.created_timestamp", "timeframes": ["date", "week", "month"]}
        ]

        processed_dims = generator._comment_conflicting_dimensions(dimensions, dimension_groups, "test_model")

        assert len(processed_dims) == 2

        # Both should be renamed with _conflict suffix
        names = [dim['name'] for dim in processed_dims]
        assert 'created_date_conflict' in names
        assert 'created_week_conflict' in names

    def test_comment_conflicting_dimensions_no_conflicts(self, generator):
        """Test that non-conflicting dimensions are left unchanged."""
        dimensions = [
            {"name": "user_id", "type": "string", "sql": "${TABLE}.user_id", "description": "User identifier"},
            {"name": "status", "type": "string", "sql": "${TABLE}.status", "description": "Status field"},
        ]

        dimension_groups = [
            {"name": "created", "type": "time", "sql": "${TABLE}.created_timestamp", "timeframes": ["date", "week", "month"]}
        ]

        processed_dims = generator._comment_conflicting_dimensions(dimensions, dimension_groups, "test_model")

        assert len(processed_dims) == 2

        # All dimensions should remain unchanged
        for i, dim in enumerate(processed_dims):
            assert dim['name'] == dimensions[i]['name']
            assert dim['description'] == dimensions[i]['description']
