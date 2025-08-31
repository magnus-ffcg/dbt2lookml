from dbt2lookml.enums import LookerDateTimeframes
from dbt2lookml.generators.dimension import LookmlDimensionGenerator


class TestDimensionGroup:
    def test_dimension_group_filtering(self):
        """Test that conflicting dimension groups are filtered out."""

        # Mock CLI args
        class MockArgs:
            pass

        generator = LookmlDimensionGenerator(MockArgs())
        # Test case: existing dimensions that conflict with potential dimension groups
        dimensions = [
            {"name": "id", "type": "number"},
            {"name": "created_date", "type": "date"},  # Conflicts with "created" dimension group
            {"name": "updated_month", "type": "string"},  # Conflicts with "updated" dimension group
            {"name": "status", "type": "string"},  # No conflict
        ]
        dimension_groups = [
            {
                "name": "created",  # Would generate created_date, created_month, etc.
                "type": "time",
                "datatype": "date",
                "timeframes": list(LookerDateTimeframes.values()),
            },
            {
                "name": "updated",  # Would generate updated_date, updated_month, etc.
                "type": "time",
                "datatype": "date",
                "timeframes": list(LookerDateTimeframes.values()),
            },
            {
                "name": "processed",  # No conflicts with existing dimensions
                "type": "time",
                "datatype": "date",
                "timeframes": list(LookerDateTimeframes.values()),
            },
        ]
        # Test dimension removal approach
        processed_dimensions, conflicting_names = generator._comment_conflicting_dimensions(dimensions, dimension_groups)
        # Assert conflicting dimensions are removed
        assert len(conflicting_names) == 2  # created_date and updated_month should be removed
        assert 'created_date' in conflicting_names
        assert 'updated_month' in conflicting_names
        # Assert non-conflicting dimensions are preserved
        remaining_names = {dim['name'] for dim in processed_dimensions}
        assert 'id' in remaining_names
        assert 'status' in remaining_names
        assert 'created_date' not in remaining_names
        assert 'updated_month' not in remaining_names
        # Dimension groups should remain unchanged since we remove conflicting dimensions instead
        # All dimension groups should be preserved with their original timeframes
        assert len(dimension_groups) == 3  # All original dimension groups should remain

    def test_conflict_detection(self):
        """Test the dimension conflict detection and resolution."""

        # Mock CLI args
        class MockArgs:
            pass

        generator = LookmlDimensionGenerator(MockArgs())
        # Test case: dimension group for "created" will generate "created_date", "created_month"
        dimensions = [
            {"name": "id", "type": "number"},
            {"name": "created_date", "type": "date"},  # This conflicts with dimension group
            {"name": "created_month", "type": "string"},  # This also conflicts
            {"name": "status", "type": "string"},  # This doesn't conflict
        ]
        # Use the actual timeframes that the generator uses
        actual_timeframes = ["raw", "date", "week", "month", "quarter", "year"]
        dimension_groups = [
            {
                "name": "created",
                "type": "time",
                "datatype": "date",
                "timeframes": actual_timeframes,
                "_original_column_name": "created_date",
            }
        ]
        # Test conflict detection
        existing_names = {dim["name"] for dim in dimensions}
        conflicts = generator._get_conflicting_timeframes(
            dimension_groups[0], existing_names, dimension_groups[0]["_original_column_name"]
        )
        # Test specific conflicts are detected
        expected_conflicts = {"date", "month"}
        assert (
            set(conflicts) == expected_conflicts
        ), f"Expected conflicts {expected_conflicts}, got {set(conflicts)}"
        # Test conflict resolution produces expected results
        processed_dimensions, conflicting_names = generator._comment_conflicting_dimensions(dimensions, dimension_groups)
        # Verify conflicting dimensions are removed
        assert len(conflicting_names) == 2, f"Expected 2 conflicting dimensions, got {len(conflicting_names)}"
        assert 'created_date' in conflicting_names, "Expected 'created_date' to be marked as conflicting"
        assert 'created_month' in conflicting_names, "Expected 'created_month' to be marked as conflicting"
        # Verify non-conflicting dimensions remain
        remaining_names = {dim['name'] for dim in processed_dimensions}
        assert 'id' in remaining_names, "Expected 'id' dimension to remain"
        assert 'status' in remaining_names, "Expected 'status' dimension to remain"
        assert 'created_date' not in remaining_names, "Expected 'created_date' dimension to be removed"
        assert 'created_month' not in remaining_names, "Expected 'created_month' dimension to be removed"
        # Test generated names method returns expected names
        generated_names = generator._get_dimension_group_generated_names("created", "date")
        # The actual implementation uses a simplified subset of timeframes
        expected_timeframes = ["raw", "date", "week", "month", "quarter", "year"]
        expected_names = [f"created_{tf}" for tf in expected_timeframes]
        assert (
            generated_names == expected_names
        ), f"Expected {expected_names}, got {generated_names}"
