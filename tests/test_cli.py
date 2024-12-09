import pytest
import logging
from dbt2lookml.cli import Cli
from dbt2lookml.exceptions import CliError
from unittest.mock import Mock, patch, call


@pytest.fixture(autouse=True)
def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(level=logging.INFO)
    yield
    logging.getLogger().handlers = []

def test_create_parser():
    """Test argument parser initialization"""
    parser = Cli()._init_argparser()
    args = parser.parse_args([])
    assert args.target_dir == './target'
    assert args.output_dir == '.'
    assert args.tag is None
    assert args.exposures_only is False
    assert args.skip_explore_joins is False
    assert args.use_table_name is False
    assert args.select is None
    assert args.log_level == 'INFO'

def test_parse_args_custom():
    """Test parsing arguments with custom values"""
    parser = Cli()._init_argparser()
    args = parser.parse_args([
        '--target-dir', '/custom/target',
        '--output-dir', '/custom/output',
        '--tag', 'analytics',
        '--exposures-only',
        '--skip-explore-joins',
        '--use-table-name',
        '--select', 'model_name',
        '--log-level', 'DEBUG'
    ])
    assert args.target_dir == '/custom/target'
    assert args.output_dir == '/custom/output'
    assert args.tag == 'analytics'
    assert args.exposures_only is True
    assert args.skip_explore_joins is True
    assert args.use_table_name is True
    assert args.select == 'model_name'
    assert args.log_level == 'DEBUG'
    
def test_cli_logging(mocker, caplog):
    """Test that CLI logs appropriate messages during execution"""
    caplog.set_level(logging.INFO)
    
    # Mock LookmlGenerator
    mock_generator = Mock()
    mock_generator.generate.return_value = ['view1', 'view2']
    mocker.patch('dbt2lookml.cli.LookmlGenerator', return_value=mock_generator)
    
    # Create CLI instance
    cli = Cli()
    
    # Mock argparse
    args = Mock()
    args.target_dir = './target'
    args.output_dir = './output'
    args.tag = None
    args.exposures_only = False
    args.exposures_tag = None
    args.skip_explore_joins = False
    args.select = None
    args.use_table_name = False
    args.generate_locale = False
    args.log_level = 'INFO'
    
    # Run CLI with mocked args
    cli.generate(args)
    
    # Verify logs
    assert 'Parsing dbt models (bigquery) and creating lookml views...' in caplog.text
    assert 'Generated 2 views' in caplog.text
    assert 'Success' in caplog.text
    
    # Verify generator was called with correct args
    mock_generator.generate.assert_called_once_with(
        target_dir='./target',
        output_dir='./output',
        tag=None,
        exposures_only=False,
        exposures_tag=None,
        skip_explore_joins=False,
        select=None,
        use_table_name_as_view=False,
        generate_locale=False
    )

def test_cli_error_logging(mocker, caplog):
    """Test that CLI logs appropriate messages when errors occur"""
    caplog.set_level(logging.INFO)
    
    # Create CLI instance and mock arguments
    cli = Cli()
    
    args = Mock()
    args.target_dir = 'test_dir'
    args.output_dir = 'output_dir'
    args.log_level = 'INFO'
    args.generate_locale = False
    args.table_format_sql = False
    args.exposures_tag = None
    args.exposures_only = False
    
    mock_parser = Mock()
    mock_parser.parse_args.return_value = args
    mocker.patch.object(cli, '_init_argparser', return_value=mock_parser)
    
    # Mock generate to raise error
    mocker.patch.object(cli, 'generate', side_effect=CliError("Test error"))

    cli.run()
    
    # Verify error logs
    assert 'Error occurred during generation. Stopped execution.' in caplog.text    
