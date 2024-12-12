import pytest
import logging
from unittest.mock import Mock, patch, call

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
    mock_dbt_parser.assert_called_once_with('manifest', 'catalog')
    mock_parser_instance.get_models.assert_called_once_with(args)
    
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
