import pytest
import logging
from unittest.mock import Mock, patch, call
import os

# Only import from cli.py
from dbt2lookml.cli import Cli
from dbt2lookml.exceptions import CliError


def test_create_parser_default_args():
    """Test argument parser with default values"""
    parser = Cli()._init_argparser()
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
    
    assert args.target_dir == 'target'
    assert args.output_dir == 'output'
    assert args.build_explore is True  # Default should be True (generate explores)
    assert args.use_table_name is False
    assert args.tag is None
    assert args.select is None
    assert args.log_level == 'INFO'
    assert args.exposures_only is False
    assert args.exposures_tag is None
    assert args.generate_locale is False

def test_skip_explore_flag():
    """Test --skip-explore flag behavior"""
    parser = Cli()._init_argparser()
    
    # Without --skip-explore (default)
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
    assert args.build_explore is True
    
    # With --skip-explore
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output', '--skip-explore'])
    assert args.build_explore is False

def test_parse_args_all_options():
    """Test parsing arguments with all options specified"""
    parser = Cli()._init_argparser()
    args = parser.parse_args([
        '--target-dir', '/custom/target',
        '--output-dir', '/custom/output',
        '--tag', 'analytics',
        '--exposures-only',
        '--skip-explore',
        '--use-table-name',
        '--select', 'model_name',
        '--log-level', 'DEBUG',
        '--generate-locale'
    ])
    
    assert args.target_dir == '/custom/target'
    assert args.output_dir == '/custom/output'
    assert args.tag == 'analytics'
    assert args.build_explore is False  # --skip-explore was used
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
        exposures_only=False,
        exposures_tag=None,
        tag=None,
        select=None,
        build_explore=True
    )
    result = cli.parse(args)
    
    # Verify file handler calls
    mock_file_handler_instance.read.assert_has_calls([
        call('target/manifest.json'),
        call('target/catalog.json')
    ])
    
    # Verify parser calls with correct args
    mock_dbt_parser.assert_called_once_with(args, 'manifest', 'catalog')
    mock_parser_instance.get_models.assert_called_once_with()
    
    assert result == ['model1', 'model2']

@patch('dbt2lookml.cli.LookmlGenerator')
def test_cli_generate_with_args(mock_generator):
    """Test generate method respects build_explore flag"""
    # Mock generator
    mock_generator_instance = Mock()
    mock_generator.return_value = mock_generator_instance
    mock_generator_instance.generate.return_value = ('path/to/view.lkml', {'view': {'name': 'test'}})
    
    cli = Cli()
    
    # Test with build_explore=True
    args = Mock(output_dir='output', build_explore=True)
    cli.generate(args, [Mock()])
    mock_generator.assert_called_with(args)
    
    # Test with build_explore=False
    mock_generator.reset_mock()
    args = Mock(output_dir='output', build_explore=False)
    cli.generate(args, [Mock()])
    mock_generator.assert_called_with(args)

def test_continue_on_error_flag():
    """Test --continue-on-error flag behavior"""
    parser = Cli()._init_argparser()
    
    # Without --continue-on-error (default)
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output'])
    assert not hasattr(args, 'continue_on_error') or not args.continue_on_error
    
    # With --continue-on-error
    args = parser.parse_args(['--target-dir', 'target', '--output-dir', 'output', '--continue-on-error'])
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
        Exception("Model generation failed")
    ]
    
    # Create test models
    models = [
        Mock(name='model1'),
        Mock(name='model2')
    ]
    
    cli = Cli()
    args = Mock(
        output_dir='output',
        continue_on_error=True  # Enable continue on error
    )
    
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
    assert write_args[0] == expected_path # normalize path for Windows

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
    args = Mock(
        output_dir='output',
        continue_on_error=False  # Disable continue on error
    )
    
    # Should raise exception immediately
    with pytest.raises(Exception) as exc_info:
        cli.generate(args, [Mock(name='model1')])
    
    assert "Model generation failed" in str(exc_info.value)
    assert mock_generator_instance.generate.call_count == 1
