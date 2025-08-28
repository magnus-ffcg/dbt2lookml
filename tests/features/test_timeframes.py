#!/usr/bin/env python3
"""Test script for extended timeframe support in configuration."""
import os
import sys
import tempfile

import yaml

sys.path.insert(0, os.path.dirname(__file__))
from dbt2lookml.cli import Cli
from dbt2lookml.enums import LookerTimeFrames


class MockArgs:
    def __init__(self):
        self.timeframes = None


def test_extended_timeframes_in_config():
    """Test that all LookerExtendedTimeFrames can be used in config file."""
    cli = Cli()
    # Create a temporary config file with all extended timeframes
    extended_timeframes = list(LookerTimeFrames.values())
    config_data = {'timeframes': {'date': extended_timeframes, 'time': extended_timeframes}}
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        temp_config_path = f.name
    try:
        # Test loading config
        config = cli._load_config(temp_config_path)
        # Verify all extended timeframes are accepted
        assert 'timeframes' in config
        assert 'date' in config['timeframes']
        assert 'time' in config['timeframes']
        # Check that all extended timeframes are present
        date_timeframes = config['timeframes']['date']
        time_timeframes = config['timeframes']['time']
        for timeframe in extended_timeframes:
            assert timeframe in date_timeframes, f"Missing timeframe in date: {timeframe}"
            assert timeframe in time_timeframes, f"Missing timeframe in time: {timeframe}"
        print("✓ All extended timeframes successfully loaded from config")
        print(f"✓ Total timeframes tested: {len(extended_timeframes)}")
    finally:
        # Clean up
        os.unlink(temp_config_path)


def test_extended_timeframes_merge_with_args():
    """Test that extended timeframes properly merge with CLI args."""
    cli = Cli()
    # Mock CLI args with no timeframes
    args = MockArgs()
    args.timeframes = None
    # Config with extended timeframes
    config = {
        'timeframes': {
            'date': ['raw', 'date', 'hour2', 'hour4', 'minute15'],
            'time': ['raw', 'time', 'second', 'minute30'],
        }
    }
    # Merge config with args
    merged_args = cli._merge_config_with_args(args, config)
    # Verify extended timeframes are preserved
    assert merged_args.timeframes['date'] == ['raw', 'date', 'hour2', 'hour4', 'minute15']
    assert merged_args.timeframes['time'] == ['raw', 'time', 'second', 'minute30']
    print("✓ Extended timeframes properly merged with CLI args")
    print(f"✓ Date timeframes: {merged_args.timeframes['date']}")
    print(f"✓ Time timeframes: {merged_args.timeframes['time']}")


def test_individual_extended_timeframes():
    """Test individual extended timeframe values."""
    cli = Cli()
    # Test specific extended timeframes
    test_cases = [
        ('hour2', 'hour2'),
        ('hour6', 'hour6'),
        ('minute15', 'minute15'),
        ('minute30', 'minute30'),
        ('second', 'second'),
        ('day_of_week', 'day_of_week'),
        ('fiscal_quarter', 'fiscal_quarter'),
    ]
    for timeframe_key, expected_value in test_cases:
        config = {'timeframes': {'date': [timeframe_key], 'time': [timeframe_key]}}
        args = MockArgs()
        merged_args = cli._merge_config_with_args(args, config)
        assert timeframe_key in merged_args.timeframes['date']
        assert timeframe_key in merged_args.timeframes['time']
        print(f"✓ {timeframe_key} timeframe accepted")


if __name__ == "__main__":
    test_extended_timeframes_in_config()
    test_extended_timeframes_merge_with_args()
    test_individual_extended_timeframes()
    print("\n🎉 All extended timeframe tests passed!")
