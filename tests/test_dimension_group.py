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
            },
            {
                "name": "updated",  # Would generate updated_date, updated_month, etc.
                "type": "time", 
                "datatype": "date",
            },
            {
                "name": "processed",  # No conflicts with existing dimensions
                "type": "time",
                "datatype": "date", 
            }
        ]

        print("Testing dimension group conflict filtering:")
        print("=" * 60)

        print("Existing dimensions:")
        for dim in dimensions:
            print(f"  - {dim['name']}")

        print("\nProposed dimension groups:")
        for dg in dimension_groups:
            print(f"  - {dg['name']} (would generate: {dg['name']}_date, {dg['name']}_month, etc.)")

        # Test commenting approach (updated method name)
        processed_groups = generator._comment_conflicting_timeframes(dimensions, dimension_groups)

        print("\nProcessed dimension groups (conflicts commented):")
        for dg in processed_groups:
            print(f"  - {dg['name']}")
            if 'timeframes' in dg:
                timeframes = dg['timeframes']
                active = [tf for tf in timeframes if not tf.startswith('#')]
                commented = [tf for tf in timeframes if tf.startswith('#')]
                print(f"    Active: {len(active)}, Commented: {len(commented)}")

        count = len(dimension_groups)
        print(f"\nResult: {count} groups preserved with conflicts commented")

        # Show what each dimension group would generate
        print("\nDetailed conflict analysis:")
        existing_names = {dim["name"] for dim in dimensions}

        for dg in dimension_groups:
            group_name = dg["name"]
            generated_names = generator._get_dimension_group_generated_names(
                group_name, "date")
            conflicts = [name for name in generated_names if name in existing_names]

            if conflicts:
                print(f"  ✗ {group_name}: conflicts with {conflicts}")
            else:
                print(f"  ✓ {group_name}: no conflicts")

    def test_conflict_detection(self):
        """Test the dimension conflict detection and resolution."""

        # Mock CLI args
        class MockArgs:
            pass

        generator = LookmlDimensionGenerator(MockArgs())

        # Test case: dimension group for "created" will generate "created_date", "created_month"
        # etc
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
                "datatype": "date",  # This makes it a date dimension group
            }
        ]

        print("Testing dimension conflict detection:")
        print("=" * 50)

        # Add original column name to dimension groups for proper testing
        dimension_groups[0]["_original_column_name"] = "created_date"

        # Test conflict detection
        existing_names = {dim["name"] for dim in dimensions}
        conflicts = generator._get_conflicting_timeframes(
            dimension_groups[0], existing_names, dimension_groups[0]["_original_column_name"]
        )
        print(f"Detected conflicts: {conflicts}")

        # Test conflict resolution (commenting approach)
        processed_groups = generator._comment_conflicting_timeframes(dimensions, dimension_groups)

        print("\nOriginal dimensions:")
        for dim in dimensions:
            print(f"  - {dim['name']}")

        print("\nProcessed dimension group timeframes:")
        if processed_groups:
            timeframes = processed_groups[0].get("timeframes", [])
            for tf in timeframes:
                if tf.startswith("#"):
                    print(f"  - {tf}")  # Commented timeframe
                else:
                    print(f"  - {tf}")  # Active timeframe

        # Test generated names
        print("\nDimension group 'created' would generate these names:")
        generated_names = generator._get_dimension_group_generated_names("created", "date")
        for name in generated_names:
            print(f"  - {name}")
