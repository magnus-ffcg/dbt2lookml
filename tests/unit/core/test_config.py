"""Tests for configuration file support in CLI."""

import os
import tempfile
from unittest.mock import Mock

import pytest
import yaml

from dbt2lookml.cli import Cli
from dbt2lookml.exceptions import CliError


class TestConfigSupport:
    """Test configuration file support."""

    def test_load_config_valid_yaml(self):
        """Test loading valid YAML configuration."""
        cli = Cli()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'output_dir': '/custom/output',
                'tag': 'analytics',
                'timeframes': {
                    'date': ['date', 'week', 'month', 'year'],
                    'time': ['raw', 'time', 'date', 'hour'],
                },
            }
            yaml.dump(config_data, f)
            config_path = f.name
        try:
            config = cli._load_config(config_path)
            assert config['output_dir'] == '/custom/output'
            assert config['tag'] == 'analytics'
            assert config['timeframes']['date'] == ['date', 'week', 'month', 'year']
            assert config['timeframes']['time'] == ['raw', 'time', 'date', 'hour']
        finally:
            os.unlink(config_path)

    def test_load_config_nonexistent_file(self):
        """Test loading non-existent configuration file."""
        cli = Cli()
        with pytest.raises(CliError) as exc_info:
            cli._load_config('/nonexistent/config.yaml')
        assert "Configuration file not found" in str(exc_info.value)

    def test_load_config_invalid_yaml(self):
        """Test loading invalid YAML configuration."""
        cli = Cli()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('invalid: yaml: [')
            config_path = f.name
        try:
            with pytest.raises(CliError) as exc_info:
                cli._load_config(config_path)
            assert "Error parsing YAML configuration" in str(exc_info.value)
        finally:
            os.unlink(config_path)

    def test_merge_config_with_args_cli_precedence(self):
        """Test that CLI arguments take precedence over config file."""
        cli = Cli()
        # Create mock args - include_explore=False is the new default
        args = Mock()
        args.output_dir = '/cli/output'
        args.tag = None
        args.include_explore = True  # CLI explicitly set to True
        config = {
            'output_dir': '/config/output',
            'tag': 'config_tag',
            'include_explore': False,  # Config wants False
        }
        merged_args = cli._merge_config_with_args(args, config)
        # CLI args should take precedence
        assert merged_args.output_dir == '/cli/output'
        # Config should fill in None values
        assert merged_args.tag == 'config_tag'
        # CLI explicit value should override config
        assert merged_args.include_explore is True

    def test_merge_config_with_args_config_fallback(self):
        """Test config file values are used when CLI args are defaults."""
        cli = Cli()
        # Create mock args with default values
        args = Mock()
        args.output_dir = '.'
        args.tag = None
        args.include_explore = False  # Using default value
        config = {
            'output_dir': '/config/output',
            'tag': 'config_tag',
            'include_explore': True,  # Config wants True
        }
        merged_args = cli._merge_config_with_args(args, config)
        # Config should override defaults
        assert merged_args.output_dir == '/config/output'
        assert merged_args.tag == 'config_tag'
        assert merged_args.include_explore is True

    def test_merge_config_with_custom_timeframes(self):
        """Test custom timeframes are properly merged."""
        cli = Cli()
        args = Mock()
        args.timeframes = None
        config = {'timeframes': {'date': ['date', 'month'], 'time': ['raw', 'time', 'hour']}}
        merged_args = cli._merge_config_with_args(args, config)
        assert merged_args.timeframes['date'] == ['date', 'month']
        assert merged_args.timeframes['time'] == ['raw', 'time', 'hour']

    def test_config_only_mode(self):
        """Test that config file can be the only parameter."""
        cli = Cli()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'target_dir': '/dbt/target',
                'output_dir': '/lookml/output',
                'tag': 'analytics',
                'log_level': 'DEBUG',
            }
            yaml.dump(config_data, f)
            config_path = f.name
        try:
            # Parse args with only --config
            args = cli._args_parser.parse_args(['--config', config_path])
            assert args.config == config_path
        finally:
            os.unlink(config_path)

    def test_cli_argparser_has_config_option(self):
        """Test that CLI argument parser includes --config option."""
        cli = Cli()
        parser = cli._init_argparser()
        # Check that --config is in help
        help_text = parser.format_help()
        assert '--config' in help_text
        assert 'Path to YAML configuration file' in help_text

    def test_run_with_config_file(self):
        """Test CLI config loading and merging."""
        cli = Cli()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'target_dir': '/test/target',
                'output_dir': '/test/output',
                'log_level': 'DEBUG',
            }
            yaml.dump(config_data, f)
            config_path = f.name
        try:
            # Test config loading directly
            loaded_config = cli._load_config(config_path)
            # Verify config was loaded correctly
            assert loaded_config['target_dir'] == '/test/target'
            assert loaded_config['output_dir'] == '/test/output'
            assert loaded_config['log_level'] == 'DEBUG'
        finally:
            os.unlink(config_path)
