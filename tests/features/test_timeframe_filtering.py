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
    # Test dimension removal (updated approach)
    processed_dimensions, conflicting_names = generator._comment_conflicting_dimensions(dimensions, dimension_groups)
    
    print(f"\nProcessed dimensions ({len(processed_dimensions)}):")
    for dim in processed_dimensions:
        print(f"  - {dim['name']}")
    
    print(f"\nConflicting dimensions removed ({len(conflicting_names)}):")
    for name in conflicting_names:
        print(f"  - {name}")
    
    print("\nSummary:")
    print(f"  - Remaining dimensions: {len(processed_dimensions)}")
    print(f"  - Removed conflicting dimensions: {len(conflicting_names)}")
    
    # Test that conflicting dimensions are removed
    assert len(conflicting_names) == 2, f"Expected 2 conflicting dimensions, got {len(conflicting_names)}"
    assert 'created_date' in conflicting_names, "Expected 'created_date' to be removed"
    assert 'created_month' in conflicting_names, "Expected 'created_month' to be removed"
    
    # Test that non-conflicting dimensions remain
    remaining_names = {dim['name'] for dim in processed_dimensions}
    assert 'id' in remaining_names, "Expected 'id' dimension to remain"
    assert 'status' in remaining_names, "Expected 'status' dimension to remain"
    assert 'created_date' not in remaining_names, "Expected 'created_date' to be removed"
    assert 'created_month' not in remaining_names, "Expected 'created_month' to be removed"


if __name__ == "__main__":
    test_timeframe_filtering()
