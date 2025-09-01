"""Unit tests for CLI module."""

import argparse
import os
import pytest
from unittest.mock import Mock, patch, mock_open

from dbt2lookml.cli import Cli
from dbt2lookml.exceptions import CliError


class TestCliUnit:
    """Unit tests for Cli class methods."""

    def test_init(self):
        """Test Cli initialization."""
        cli = Cli()
        assert cli._args_parser is not None
        assert cli._file_handler is not None

    def test_get_config_with_defaults(self):
        """Test _get_config_with_defaults returns expected defaults."""
        cli = Cli()
        defaults = cli._get_config_with_defaults()
        
        assert defaults['target_dir'] == '.'
        assert defaults['output_dir'] == '.'
        assert defaults['log_level'] == 'INFO'
        assert defaults['exposures_only'] is False
        assert defaults['use_table_name'] is False
        assert defaults['include_explore'] is False
        assert defaults['include_iso_fields'] is False
        assert defaults['continue_on_error'] is False
        assert defaults['generate_locale'] is False
        assert defaults['tag'] is None
        assert defaults['select'] is None
        assert defaults['manifest_path'] is None
        assert defaults['catalog_path'] is None

    def test_merge_config_with_args_defaults_only(self):
        """Test _merge_config_with_args with only defaults."""
        cli = Cli()
        args = argparse.Namespace(
            target_dir='.',
            output_dir='.',
            tag=None,
            log_level='INFO',
        )
        config_file = {}
        
        result = cli._merge_config_with_args(args, config_file)
        
        assert result.target_dir == '.'
        assert result.output_dir == '.'
        assert result.tag is None
        assert result.log_level == 'INFO'

    def test_merge_config_with_args_config_overrides_defaults(self):
        """Test _merge_config_with_args where config overrides defaults."""
        cli = Cli()
        args = argparse.Namespace(
            target_dir='.',  # Default
            output_dir='.',  # Default
            tag=None,  # Default
            log_level='INFO',  # Default
        )
        config_file = {
            'target_dir': '/config/target',
            'tag': 'config_tag',
            'log_level': 'DEBUG',
        }
        
        result = cli._merge_config_with_args(args, config_file)
        
        assert result.target_dir == '/config/target'
        assert result.tag == 'config_tag'
        assert result.log_level == 'DEBUG'
        assert result.output_dir == '.'  # Not in config, keeps default

    def test_merge_config_with_args_cli_overrides_config(self):
        """Test _merge_config_with_args where CLI args override config."""
        cli = Cli()
        args = argparse.Namespace(
            target_dir='/cli/target',  # Non-default CLI value
            output_dir='.',  # Default
            tag='cli_tag',  # Non-default CLI value
            log_level='INFO',  # Default
        )
        config_file = {
            'target_dir': '/config/target',
            'tag': 'config_tag',
            'log_level': 'DEBUG',
        }
        
        result = cli._merge_config_with_args(args, config_file)
        
        # CLI args should override config
        assert result.target_dir == '/cli/target'
        assert result.tag == 'cli_tag'
        # Config should override defaults when CLI has default
        assert result.log_level == 'DEBUG'

    def test_merge_config_with_args_unknown_config_keys(self):
        """Test _merge_config_with_args ignores unknown config keys."""
        cli = Cli()
        args = argparse.Namespace(
            target_dir='.',
            output_dir='.',
        )
        config_file = {
            'unknown_key': 'unknown_value',
            'target_dir': '/config/target',
        }
        
        result = cli._merge_config_with_args(args, config_file)
        
        assert result.target_dir == '/config/target'
        assert not hasattr(result, 'unknown_key')

    @patch('builtins.open', new_callable=mock_open, read_data='target_dir: /yaml/target\nlog_level: DEBUG')
    @patch('yaml.safe_load')
    def test_load_config_success(self, mock_yaml_load, mock_file):
        """Test successful config loading."""
        mock_yaml_load.return_value = {
            'target_dir': '/yaml/target',
            'log_level': 'DEBUG'
        }
        
        cli = Cli()
        result = cli._load_config('/path/to/config.yaml')
        
        mock_file.assert_called_once_with('/path/to/config.yaml', 'r')
        assert result['target_dir'] == '/yaml/target'
        assert result['log_level'] == 'DEBUG'

    def test_load_config_file_not_found(self):
        """Test config loading with missing file."""
        cli = Cli()
        
        with pytest.raises(CliError) as exc_info:
            cli._load_config('/nonexistent/config.yaml')
        
        assert "Configuration file not found" in str(exc_info.value)

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    def test_load_config_yaml_error(self, mock_yaml_load, mock_file):
        """Test config loading with YAML parsing error."""
        import yaml
        mock_yaml_load.side_effect = yaml.YAMLError("Invalid YAML syntax")
        
        cli = Cli()
        
        with pytest.raises(CliError) as exc_info:
            cli._load_config('/path/to/config.yaml')
        
        assert "Error parsing YAML configuration" in str(exc_info.value)

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    def test_load_config_empty_file(self, mock_yaml_load, mock_file):
        """Test config loading with empty file."""
        mock_yaml_load.return_value = None
        
        cli = Cli()
        result = cli._load_config('/path/to/config.yaml')
        
        assert result == {}

    @patch('os.makedirs')
    def test_write_lookml_file_success(self, mock_makedirs):
        """Test successful _write_lookml_file."""
        cli = Cli()
        cli._file_handler = Mock()
        
        result = cli._write_lookml_file('/output', 'models/test.view.lkml', 'view content')
        
        # Should create directory and write file
        mock_makedirs.assert_called_once_with('output/models', exist_ok=True)
        cli._file_handler.write.assert_called_once_with('output/models/test.view.lkml', 'view content')
        assert result == 'output/models/test.view.lkml'

    def test_write_lookml_file_os_error(self):
        """Test _write_lookml_file with OS error."""
        cli = Cli()
        cli._file_handler = Mock()
        cli._file_handler.write.side_effect = OSError("Permission denied")
        
        with pytest.raises(CliError) as exc_info:
            cli._write_lookml_file('/output', 'test.view.lkml', 'view content')
        
        assert "Failed to write file" in str(exc_info.value)
        assert "Permission denied" in str(exc_info.value)

    @patch('dbt2lookml.cli.DbtParser')
    def test_parse_with_custom_paths(self, mock_dbt_parser):
        """Test parse method with custom manifest and catalog paths."""
        cli = Cli()
        cli._file_handler = Mock()
        cli._file_handler.read.side_effect = ['manifest_content', 'catalog_content']
        
        mock_parser_instance = Mock()
        mock_dbt_parser.return_value = mock_parser_instance
        mock_parser_instance.get_models.return_value = ['model1']
        
        args = Mock(
            manifest_path='/custom/manifest.json',
            catalog_path='/custom/catalog.json',
            target_dir='target',
            exposures_only=False,
            exposures_tag=None,
            tag=None,
            select=None,
            include_explore=True,
        )
        
        result = cli.parse(args)
        
        cli._file_handler.read.assert_any_call('/custom/manifest.json')
        cli._file_handler.read.assert_any_call('/custom/catalog.json')
        assert result == ['model1']

    @patch('dbt2lookml.cli.DbtParser')
    def test_parse_with_default_paths(self, mock_dbt_parser):
        """Test parse method with default manifest and catalog paths."""
        cli = Cli()
        cli._file_handler = Mock()
        cli._file_handler.read.side_effect = ['manifest_content', 'catalog_content']
        
        mock_parser_instance = Mock()
        mock_dbt_parser.return_value = mock_parser_instance
        mock_parser_instance.get_models.return_value = ['model1']
        
        args = Mock(
            manifest_path=None,
            catalog_path=None,
            target_dir='/target',
            exposures_only=False,
            exposures_tag=None,
            tag=None,
            select=None,
            include_explore=True,
        )
        
        result = cli.parse(args)
        
        cli._file_handler.read.assert_any_call('/target/manifest.json')
        cli._file_handler.read.assert_any_call('/target/catalog.json')

    def test_parse_file_read_error(self):
        """Test parse method with file read error."""
        cli = Cli()
        cli._file_handler = Mock()
        cli._file_handler.read.side_effect = FileNotFoundError("File not found")
        
        args = Mock(
            target_dir='target',
            manifest_path=None,
            catalog_path=None,
        )
        
        with pytest.raises(CliError) as exc_info:
            cli.parse(args)
        
        assert "Failed to read file" in str(exc_info.value)

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_single_model_success(self, mock_generate_single):
        """Test generate method with single model success."""
        cli = Cli()
        
        mock_generate_single.return_value = '/output/models/test.view.lkml'
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test_model')]
        
        result = cli.generate(args, models)
        
        assert len(result) == 1
        assert result[0] == '/output/models/test.view.lkml'

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_multiple_models_success(self, mock_generate_single):
        """Test generate method with multiple models success."""
        cli = Cli()
        
        mock_generate_single.side_effect = [
            '/output/models/test1.view.lkml',
            '/output/models/test2.view.lkml',
        ]
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test1'), Mock(name='test2')]
        
        result = cli.generate(args, models)
        
        assert len(result) == 2
        assert '/output/models/test1.view.lkml' in result
        assert '/output/models/test2.view.lkml' in result

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_generation_error_no_continue(self, mock_generate_single):
        """Test generate method with error and continue_on_error=False."""
        cli = Cli()
        
        mock_generate_single.side_effect = Exception("Generation failed")
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test_model')]
        
        with pytest.raises(Exception) as exc_info:
            cli.generate(args, models)
        
        assert "Generation failed" in str(exc_info.value)

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_generation_error_continue(self, mock_generate_single):
        """Test generate method with error and continue_on_error=True."""
        cli = Cli()
        
        mock_generate_single.side_effect = [
            '/output/models/test1.view.lkml',
            Exception("Generation failed for model2"),
            '/output/models/test3.view.lkml',
        ]
        
        args = Mock(output_dir='/output', continue_on_error=True, use_table_name=False)
        models = [Mock(name='test1'), Mock(name='test2'), Mock(name='test3')]
        
        result = cli.generate(args, models)
        
        # Should have 2 successful results, skip the failed one
        assert len(result) == 2
        assert '/output/models/test1.view.lkml' in result
        assert '/output/models/test3.view.lkml' in result

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_write_error_no_continue(self, mock_generate_single):
        """Test generate method with write error and continue_on_error=False."""
        cli = Cli()
        
        mock_generate_single.side_effect = CliError("Failed to write file: Write failed")
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test_model')]
        
        with pytest.raises(Exception) as exc_info:
            cli.generate(args, models)
        
        assert "Failed to write file" in str(exc_info.value) or "Write failed" in str(exc_info.value)

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_write_error_continue(self, mock_generate_single):
        """Test generate method with write error and continue_on_error=True."""
        cli = Cli()
        
        mock_generate_single.side_effect = [
            '/output/models/test1.view.lkml',  # Success
            None,  # Write error (returns None)
            '/output/models/test3.view.lkml',  # Success
        ]
        
        args = Mock(output_dir='/output', continue_on_error=True, use_table_name=False)
        models = [Mock(name='test1'), Mock(name='test2'), Mock(name='test3')]
        
        result = cli.generate(args, models)
        
        # Should have 2 successful results (test1 and test3), skip test2 due to write error
        assert len(result) == 2
        assert '/output/models/test1.view.lkml' in result
        assert '/output/models/test3.view.lkml' in result

    def test_generate_empty_models_list(self):
        """Test generate method with empty models list."""
        cli = Cli()
        cli._file_handler = Mock()
        
        args = Mock(output_dir='/output')
        models = []
        
        result = cli.generate(args, models)
        
        assert result == []
        cli._file_handler.write.assert_not_called()

    @patch('dbt2lookml.cli.logging')
    @patch('dbt2lookml.cli.DbtParser')
    def test_run_with_config_file(self, mock_dbt_parser, mock_logging):
        """Test run method with configuration file."""
        cli = Cli()
        cli._file_handler = Mock()
        cli._file_handler.read.side_effect = ['manifest', 'catalog']
        
        # Mock parser
        mock_parser_instance = Mock()
        mock_dbt_parser.return_value = mock_parser_instance
        mock_parser_instance.get_models.return_value = []
        
        # Mock config loading and merging
        cli._load_config = Mock(return_value={'log_level': 'DEBUG'})
        cli._merge_config_with_args = Mock(side_effect=lambda args, config: args)
        
        # Mock argument parsing
        mock_args = Mock(
            config='/path/to/config.yaml',
            log_level='DEBUG',
            target_dir='target',
            exposures_only=False,
            exposures_tag=None,
            tag=None,
            select=None,
            include_explore=True,
            output_dir='output'
        )
        cli._args_parser.parse_args = Mock(return_value=mock_args)
        
        cli.run()
        
        # Verify config was loaded and merged
        cli._load_config.assert_called_once_with('/path/to/config.yaml')
        cli._merge_config_with_args.assert_called_once()

    @patch('dbt2lookml.cli.logging')
    def test_run_without_config_file(self, mock_logging):
        """Test run method without configuration file."""
        cli = Cli()
        cli._file_handler = Mock()
        
        # Mock parse and generate methods
        cli.parse = Mock(return_value=[])
        cli.generate = Mock(return_value=[])
        
        # Mock argument parsing without config
        mock_args = Mock(
            config=None,
            log_level='INFO',
        )
        cli._args_parser.parse_args = Mock(return_value=mock_args)
        
        cli.run()
        
        # Should call parse but not generate when no models found
        cli.parse.assert_called_once_with(mock_args)
        # Generate is not called when models list is empty
        cli.generate.assert_not_called()

    def test_init_argparser_creates_parser(self):
        """Test _init_argparser creates argument parser."""
        cli = Cli()
        parser = cli._init_argparser()
        
        assert isinstance(parser, argparse.ArgumentParser)
        assert parser.description is not None

    def test_init_argparser_has_required_arguments(self):
        """Test _init_argparser includes required arguments."""
        cli = Cli()
        parser = cli._init_argparser()
        
        # Test that required arguments can be parsed
        args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
        assert args.target_dir == 'target'
        assert args.output_dir == 'output'

    def test_init_argparser_has_optional_arguments(self):
        """Test _init_argparser includes optional arguments."""
        cli = Cli()
        parser = cli._init_argparser()
        
        # Test optional arguments
        args = parser.parse_args([
            '--target-dir', 'target',
            '--output-dir', 'output',
            '--tag', 'analytics',
            '--log-level', 'DEBUG',
            '--include-explore',
            '--use-table-name',
            '--continue-on-error',
            '--generate-locale',
        ])
        
        assert args.tag == 'analytics'
        assert args.log_level == 'DEBUG'
        assert args.include_explore is True
        assert args.use_table_name is True
        assert args.continue_on_error is True
        assert args.generate_locale is True

    def test_init_argparser_version_argument(self):
        """Test _init_argparser includes version argument."""
        cli = Cli()
        parser = cli._init_argparser()
        
        with pytest.raises(SystemExit):
            parser.parse_args(['--version'])

    @patch('dbt2lookml.validation.LookMLValidator')
    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_validation_success(self, mock_generate_single, mock_validator_class):
        """Test generate method with successful validation."""
        cli = Cli()
        
        # Mock validation success
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        mock_validator.validate_lookml_string.return_value = {'valid': True, 'errors': []}
        
        mock_generate_single.return_value = '/output/models/test.view.lkml'
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test_model')]
        
        result = cli.generate(args, models)
        
        assert len(result) == 1
        assert result[0] == '/output/models/test.view.lkml'

    @patch('dbt2lookml.validation.LookMLValidator')
    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_validation_failure_no_continue(self, mock_generate_single, mock_validator_class):
        """Test generate method with validation failure and continue_on_error=False."""
        cli = Cli()
        
        # Mock validation failure
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        mock_validator.validate_lookml_string.return_value = {'valid': False, 'errors': ['Syntax error']}
        
        mock_generate_single.return_value = 'validation_failed'
        
        args = Mock(output_dir='/output', continue_on_error=False, use_table_name=False)
        models = [Mock(name='test_model')]
        
        result = cli.generate(args, models)
        
        # Should return empty list when validation fails
        assert len(result) == 0

    @patch('dbt2lookml.cli.Cli._generate_single_model')
    def test_generate_with_validation_failure_continue(self, mock_generate_single):
        """Test generate method with validation failure and continue_on_error=True."""
        cli = Cli()
        
        # Mock mixed results - success, validation failure, success
        mock_generate_single.side_effect = [
            '/output/models/test1.view.lkml',  # Success
            'validation_failed',  # Validation failure
            '/output/models/test3.view.lkml',  # Success
        ]
        
        args = Mock(output_dir='/output', continue_on_error=True, use_table_name=False)
        models = [Mock(name='test1'), Mock(name='test2'), Mock(name='test3')]
        
        result = cli.generate(args, models)
        
        # Should have 2 successful results (test1 and test3)
        assert len(result) == 2
        assert '/output/models/test1.view.lkml' in result
        assert '/output/models/test3.view.lkml' in result
