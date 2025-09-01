import os
from unittest.mock import Mock, call, patch

import pytest
import yaml

from dbt2lookml.cli import Cli
from dbt2lookml.exceptions import CliError


def test_create_parser_default_args():
    """Test argument parser with default values"""
    parser = Cli()._init_argparser()
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
    assert args.target_dir == 'target'
    assert args.output_dir == 'output'
    assert args.use_table_name is False
    assert args.tag is None
    assert args.select is None
    assert args.log_level == 'INFO'
    assert args.exposures_only is False
    assert args.exposures_tag is None
    assert args.generate_locale is False


def test_parse_args_all_options():
    """Test parsing arguments with all options specified"""
    parser = Cli()._init_argparser()
    args = parser.parse_args(
        [
            '--target-dir',
            '/custom/target',
            '--output-dir',
            '/custom/output',
            '--tag',
            'analytics',
            '--exposures-only',
            '--use-table-name',
            '--select',
            'model_name',
            '--log-level',
            'DEBUG',
            '--generate-locale',
        ]
    )
    assert args.target_dir == '/custom/target'
    assert args.output_dir == '/custom/output'
    assert args.tag == 'analytics'
    assert args.use_table_name is True
    assert args.select == 'model_name'
    assert args.log_level == 'DEBUG'
    assert args.exposures_only is True
    assert args.generate_locale is True


@patch('dbt2lookml.cli.DbtParser')
@patch('dbt2lookml.cli.FileHandler')
def test_cli_parse(mock_file_handler, mock_dbt_parser):
    """Test CLI parse method with different argument combinations"""
    # Mock file handler
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    mock_file_handler_instance.read.side_effect = ['manifest', 'catalog']
    # Mock dbt parser
    mock_parser_instance = Mock()
    mock_dbt_parser.return_value = mock_parser_instance
    mock_parser_instance.get_models.return_value = ['model1', 'model2']
    # Test with default args
    cli = Cli()
    args = Mock(
        target_dir='target',
        manifest_path=None,
        catalog_path=None,
        exposures_only=False,
        exposures_tag=None,
        tag=None,
        select=None
    )
    result = cli.parse(args)
    # Verify file handler calls
    mock_file_handler_instance.read.assert_has_calls(
        [call('target/manifest.json'), call('target/catalog.json')]
    )
    # Verify parser calls with correct args
    mock_dbt_parser.assert_called_once_with(args, 'manifest', 'catalog')
    mock_parser_instance.get_models.assert_called_once_with()
    assert result == ['model1', 'model2']


@patch('dbt2lookml.cli.LookmlGenerator')
def test_cli_generate_with_args(mock_generator):
    """Test generate method creates generator correctly"""
    # Mock generator
    mock_generator_instance = Mock()
    mock_generator.return_value = mock_generator_instance
    mock_generator_instance.generate.return_value = (
        'test.view.lkml',
        {'view': {'name': 'test'}},
    )
    cli = Cli()
    args = Mock(output_dir='output')
    cli.generate(args, [Mock()])
    mock_generator.assert_called_with(args)


def test_continue_on_error_flag():
    """Test --continue-on-error flag behavior"""
    parser = Cli()._init_argparser()
    # Without --continue-on-error (default)
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
    assert not hasattr(args, 'continue_on_error') or not args.continue_on_error
    # With --continue-on-error
    args = parser.parse_args(
        ['--target-dir', 'target', '--output-dir', 'output', '--continue-on-error']
    )
    assert args.continue_on_error is True


@patch('dbt2lookml.cli.LookmlGenerator')
@patch('dbt2lookml.cli.FileHandler')
def test_generate_with_empty_models(mock_file_handler, mock_generator):
    """Test generate method with empty models list"""
    # Setup mock
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    cli = Cli()
    args = Mock(output_dir='output')
    result = cli.generate(args, [])
    assert result == []
    mock_generator.assert_not_called()
    # FileHandler is called in constructor, but write should not be called
    mock_file_handler_instance.write.assert_not_called()


@patch('dbt2lookml.cli.LookmlGenerator')
@patch('dbt2lookml.cli.FileHandler')
def test_generate_with_continue_on_error(mock_file_handler, mock_generator):
    """Test generate method with --continue-on-error when some models fail"""
    # Setup mocks
    mock_generator_instance = Mock()
    mock_generator.return_value = mock_generator_instance
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    # First model succeeds, second fails
    mock_generator_instance.generate.side_effect = [
        ('model1/test1.view.lkml', {'view': [{'name': 'test'}]}),
        Exception("Model generation failed"),
    ]
    # Create test models
    models = [Mock(name='model1'), Mock(name='model2')]
    cli = Cli()
    args = Mock(output_dir='output', continue_on_error=True)  # Enable continue on error
    # Mock successful file write
    mock_file_handler_instance.write.return_value = None
    # Should not raise exception, should continue processing
    views = cli.generate(args, models)
    # Should have one successful view
    assert len(views) == 1
    expected_path = os.path.join('output', 'model1', 'test1.view.lkml')
    # Both models should have been attempted
    assert mock_generator_instance.generate.call_count == 2
    # File write should be called once for the successful model
    mock_file_handler_instance.write.assert_called_once()
    # Verify the write call arguments
    write_args = mock_file_handler_instance.write.call_args[0]
    assert write_args[0] == expected_path  # normalize path for Windows


@patch('dbt2lookml.cli.FileHandler')
def test_write_lookml_file_handles_errors(mock_file_handler):
    """Test _write_lookml_file error handling"""
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    # Test OS error
    mock_file_handler_instance.write.side_effect = OSError("Permission denied")
    cli = Cli()
    with pytest.raises(CliError) as exc_info:
        cli._write_lookml_file('output', 'test.view.lkml', 'content')
    assert "Failed to write file" in str(exc_info.value)
    assert "Permission denied" in str(exc_info.value)


@patch('dbt2lookml.cli.DbtParser')
@patch('dbt2lookml.cli.FileHandler')
def test_parse_handles_missing_files(mock_file_handler, mock_dbt_parser):
    """Test parse method handles missing manifest/catalog files"""
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    mock_file_handler_instance.read.side_effect = FileNotFoundError("manifest.json not found")
    cli = Cli()
    args = Mock(target_dir='target')
    with pytest.raises(CliError) as exc_info:
        cli.parse(args)
    assert "Failed to read file" in str(exc_info.value)
    assert "manifest.json not found" in str(exc_info.value)


@patch('dbt2lookml.cli.LookmlGenerator')
@patch('dbt2lookml.cli.FileHandler')
def test_generate_without_continue_on_error(mock_file_handler, mock_generator):
    """Test generate method without --continue-on-error when a model fails"""
    # Setup mocks
    mock_generator_instance = Mock()
    mock_generator.return_value = mock_generator_instance
    mock_generator_instance.generate.side_effect = Exception("Model generation failed")
    cli = Cli()
    args = Mock(output_dir='output', continue_on_error=False)  # Disable continue on error
    # Should raise exception immediately
    with pytest.raises(Exception) as exc_info:
        cli.generate(args, [Mock(name='model1')])
    assert "Model generation failed" in str(exc_info.value)
    assert mock_generator_instance.generate.call_count == 1


def test_version_argument():
    """Test --version argument displays version and exits"""
    parser = Cli()._init_argparser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(['--version'])
    # SystemExit with code 0 indicates successful version display
    assert exc_info.value.code == 0


def test_include_exclude_models_parsing():
    """Test --include-models and --exclude-models argument parsing"""
    parser = Cli()._init_argparser()
    
    # Test include-models
    args = parser.parse_args([
        '--target-dir', 'target',
        '--output-dir', 'output',
        '--include-models', 'model1', 'model2', 'model3'
    ])
    assert args.include_models == ['model1', 'model2', 'model3']
    
    # Test exclude-models
    args = parser.parse_args([
        '--target-dir', 'target', 
        '--output-dir', 'output',
        '--exclude-models', 'test_model', 'staging_model'
    ])
    assert args.exclude_models == ['test_model', 'staging_model']
    
    # Test both together
    args = parser.parse_args([
        '--target-dir', 'target',
        '--output-dir', 'output', 
        '--include-models', 'model1', 'model2',
        '--exclude-models', 'test_model'
    ])
    assert args.include_models == ['model1', 'model2']
    assert args.exclude_models == ['test_model']


def test_remove_schema_string_parsing():
    """Test --remove-schema-string argument parsing"""
    parser = Cli()._init_argparser()
    args = parser.parse_args([
        '--target-dir', 'target',
        '--output-dir', 'output',
        '--remove-schema-string', 'staging_'
    ])
    assert args.remove_schema_string == 'staging_'


def test_config_file_parsing():
    """Test --config argument parsing"""
    parser = Cli()._init_argparser()
    args = parser.parse_args([
        '--target-dir', 'target',
        '--output-dir', 'output',
        '--config', '/path/to/config.yaml'
    ])
    assert args.config == '/path/to/config.yaml'


@patch('builtins.open')
@patch('yaml.safe_load')
def test_load_config_success(mock_yaml_load, mock_open):
    """Test successful YAML configuration loading"""
    mock_config = {
        'target_dir': '/custom/target',
        'output_dir': '/custom/output',
        'tag': 'analytics',
        'log_level': 'DEBUG'
    }
    mock_yaml_load.return_value = mock_config
    
    cli = Cli()
    result = cli._load_config('/path/to/config.yaml')
    
    mock_open.assert_called_once_with('/path/to/config.yaml', 'r')
    assert result == mock_config


def test_load_config_file_not_found():
    """Test configuration file not found error"""
    cli = Cli()
    with pytest.raises(CliError) as exc_info:
        cli._load_config('/nonexistent/config.yaml')
    assert "Configuration file not found" in str(exc_info.value)


@patch('builtins.open')
@patch('yaml.safe_load')
def test_load_config_yaml_error(mock_yaml_load, mock_open):
    """Test YAML parsing error in configuration file"""
    mock_yaml_load.side_effect = yaml.YAMLError("Invalid YAML")
    
    cli = Cli()
    with pytest.raises(CliError) as exc_info:
        cli._load_config('/path/to/config.yaml')
    assert "Error parsing YAML configuration" in str(exc_info.value)


@patch('builtins.open')
@patch('yaml.safe_load')
def test_load_config_empty_file(mock_yaml_load, mock_open):
    """Test loading empty configuration file"""
    mock_yaml_load.return_value = None
    
    cli = Cli()
    result = cli._load_config('/path/to/config.yaml')
    
    assert result == {}


def test_merge_config_with_args():
    """Test merging configuration with CLI arguments"""
    cli = Cli()
    
    # Create mock args with defaults
    args = Mock(
        target_dir='.',  # Default value
        output_dir='/custom/output',  # Non-default value
        tag=None,  # Default value
        log_level='INFO',  # Default value
        include_iso_fields=True,  # Default value
    )
    
    config = {
        'target_dir': '/config/target',
        'output_dir': '/config/output',  # Should not override CLI arg
        'tag': 'config_tag',
        'log_level': 'DEBUG',
    }
    
    merged = cli._merge_config_with_args(args, config)
    
    # Config should override defaults
    assert merged.target_dir == '/config/target'
    assert merged.tag == 'config_tag'
    assert merged.log_level == 'DEBUG'
    
    # CLI args should take precedence over config
    assert merged.output_dir == '/custom/output'
    
    assert merged.include_iso_fields is True


@patch('dbt2lookml.cli.logging')
@patch('dbt2lookml.cli.DbtParser')
@patch('dbt2lookml.cli.FileHandler')
def test_run_with_config_file(mock_file_handler, mock_dbt_parser, mock_logging):
    """Test run method with configuration file"""
    # Mock file handler
    mock_file_handler_instance = Mock()
    mock_file_handler.return_value = mock_file_handler_instance
    mock_file_handler_instance.read.side_effect = ['manifest', 'catalog']
    
    # Mock parser
    mock_parser_instance = Mock()
    mock_dbt_parser.return_value = mock_parser_instance
    mock_parser_instance.get_models.return_value = []
    
    # Mock config loading
    cli = Cli()
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
        output_dir='output'
    )
    cli._args_parser.parse_args = Mock(return_value=mock_args)
    
    cli.run()
    
    # Verify config was loaded and merged
    cli._load_config.assert_called_once_with('/path/to/config.yaml')
    cli._merge_config_with_args.assert_called_once()
