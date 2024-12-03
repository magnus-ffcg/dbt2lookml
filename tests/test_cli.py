import pytest
from unittest.mock import patch, MagicMock
from argparse import Namespace
from dbt2looker_bigquery.cli import generate, init_argparser
import sys

def test_create_parser():
    """Test argument parser initialization"""
    parser = init_argparser()
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
    parser = init_argparser()
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

