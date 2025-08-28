#!/usr/bin/env python3
"""Test script for dimension group timeframe filtering."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from dbt2lookml.enums import LookerDateTimeframes
from dbt2lookml.generators.dimension import LookmlDimensionGenerator


def test_timeframe_filtering():
    """Test that only conflicting timeframes are filtered out."""

    # Mock CLI args
    class MockArgs:
        pass

    generator = LookmlDimensionGenerator(MockArgs())
    # Test case: existing dimensions that conflict with some timeframes
    dimensions = [
        {"name": "id", "type": "number"},
        {"name": "created_date", "type": "date"},  # Conflicts with "date" timeframe
        {"name": "created_month", "type": "string"},  # Conflicts with "month" timeframe
        {"name": "status", "type": "string"},  # No conflict
    ]
    # Create dimension group with all date timeframes
    all_timeframes = list(LookerDateTimeframes.values())
    dimension_groups = [
        {
            "name": "created",
            "type": "time",
            "datatype": "date",
            "timeframes": all_timeframes,
        }
    ]
    print("Testing timeframe filtering:")
    print("=" * 60)
    print("Existing dimensions:")
    for dim in dimensions:
        print(f"  - {dim['name']}")
    print(f"\nOriginal dimension group 'created' timeframes ({len(all_timeframes)}):")
    for tf in all_timeframes:
        print(f"  - {tf}")
    # Test commenting (updated method name)
    processed_groups = generator._comment_conflicting_timeframes(dimensions, dimension_groups)
    if processed_groups:
        processed_timeframes = processed_groups[0].get("timeframes", [])
        print(f"\nProcessed dimension group 'created' timeframes ({len(processed_timeframes)}):")
        for tf in processed_timeframes:
            print(f"  - {tf}")
        # Count commented vs active
        commented = [tf for tf in processed_timeframes if tf.startswith("#")]
        active = [tf for tf in processed_timeframes if not tf.startswith("#")]
        print("\nSummary:")
        print(f"  - Active timeframes: {len(active)}")
        print(f"  - Commented timeframes: {len(commented)}")
        print(f"  - Total timeframes preserved: {len(processed_timeframes)}")
    else:
        print("\nDimension group was completely removed (all timeframes conflicted)")
    # Test conflict detection specifically
    print("\nDetailed conflict analysis:")
    conflicting = generator._get_conflicting_timeframes(
        dimension_groups[0],
        {dim["name"] for dim in dimensions},
        dimension_groups[0].get("_original_column_name"),
    )
    print(f"Conflicting timeframes: {conflicting}")


if __name__ == "__main__":
    test_timeframe_filtering()
