from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.enums import LookerDateTimeframes


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
            }
        ]

        # Test commenting approach
        processed_groups = generator._comment_conflicting_timeframes(dimensions, dimension_groups)

        # Assert all dimension groups are preserved
        assert len(processed_groups) == len(dimension_groups)
        
        # Test specific expected outcomes for each dimension group
        created_group = next(dg for dg in processed_groups if dg["name"] == "created")
        updated_group = next(dg for dg in processed_groups if dg["name"] == "updated")
        processed_group = next(dg for dg in processed_groups if dg["name"] == "processed")
        
        # "created" group should have "date" timeframe commented out (conflicts with created_date dimension)
        created_timeframes = created_group["timeframes"]
        commented_created = [tf for tf in created_timeframes if tf.startswith('#')]
        active_created = [tf for tf in created_timeframes if not tf.startswith('#')]
        
        assert len(commented_created) == 1, f"Expected exactly 1 commented timeframe for 'created', got {len(commented_created)}"
        assert any("date" in tf for tf in commented_created), "Expected 'date' timeframe to be commented out for 'created' group"
        assert "raw" in active_created, "Expected 'raw' timeframe to remain active for 'created' group"
        assert "year" in active_created, "Expected 'year' timeframe to remain active for 'created' group"
        
        # "updated" group should have "month" timeframe commented out (conflicts with updated_month dimension)
        updated_timeframes = updated_group["timeframes"]
        commented_updated = [tf for tf in updated_timeframes if tf.startswith('#')]
        active_updated = [tf for tf in updated_timeframes if not tf.startswith('#')]
        
        assert len(commented_updated) == 1, f"Expected exactly 1 commented timeframe for 'updated', got {len(commented_updated)}"
        assert any("month" in tf for tf in commented_updated), "Expected 'month' timeframe to be commented out for 'updated' group"
        assert "raw" in active_updated, "Expected 'raw' timeframe to remain active for 'updated' group"
        assert "date" in active_updated, "Expected 'date' timeframe to remain active for 'updated' group"
        
        # "processed" group should have no commented timeframes (no conflicts)
        processed_timeframes = processed_group["timeframes"]
        commented_processed = [tf for tf in processed_timeframes if tf.startswith('#')]
        active_processed = [tf for tf in processed_timeframes if not tf.startswith('#')]
        
        assert len(commented_processed) == 0, f"Expected no commented timeframes for 'processed', got {len(commented_processed)}"
        assert len(active_processed) == len(LookerDateTimeframes.values()), "Expected all timeframes to be active for 'processed' group"

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

        dimension_groups = [
            {
                "name": "created",
                "type": "time",
                "datatype": "date",
                "timeframes": list(LookerDateTimeframes.values()),
                "_original_column_name": "created_date"
            }
        ]

        # Test conflict detection
        existing_names = {dim["name"] for dim in dimensions}
        conflicts = generator._get_conflicting_timeframes(
            dimension_groups[0], existing_names, dimension_groups[0]["_original_column_name"]
        )
        
        # Test specific conflicts are detected
        expected_conflicts = {"date", "month"}
        assert set(conflicts) == expected_conflicts, f"Expected conflicts {expected_conflicts}, got {set(conflicts)}"

        # Test conflict resolution produces expected results
        processed_groups = generator._comment_conflicting_timeframes(dimensions, dimension_groups)

        # Verify exactly one dimension group is returned
        assert len(processed_groups) == 1, f"Expected 1 processed group, got {len(processed_groups)}"
        processed_group = processed_groups[0]
        assert processed_group["name"] == "created", f"Expected group name 'created', got {processed_group['name']}"
        
        # Test specific timeframes are commented out
        timeframes = processed_group["timeframes"]
        commented = [tf for tf in timeframes if tf.startswith("#")]
        active = [tf for tf in timeframes if not tf.startswith("#")]
        
        # Verify exactly 2 timeframes are commented (date and month)
        assert len(commented) == 2, f"Expected exactly 2 commented timeframes, got {len(commented)}"
        assert any("date" in tf for tf in commented), "Expected 'date' timeframe to be commented out"
        assert any("month" in tf for tf in commented), "Expected 'month' timeframe to be commented out"
        
        # Verify specific active timeframes remain
        expected_active_count = len(LookerDateTimeframes.values()) - 2  # All except date and month
        assert len(active) == expected_active_count, f"Expected {expected_active_count} active timeframes, got {len(active)}"
        assert "raw" in active, "Expected 'raw' timeframe to remain active"
        assert "year" in active, "Expected 'year' timeframe to remain active"
        assert "quarter" in active, "Expected 'quarter' timeframe to remain active"

        # Test generated names method returns expected names
        generated_names = generator._get_dimension_group_generated_names("created", "date")
        expected_names = [f"created_{tf}" for tf in LookerDateTimeframes.values()]
        assert generated_names == expected_names, f"Expected {expected_names}, got {generated_names}"
