import argparse
import json
import logging
import pathlib
import os
try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version
from rich.logging import RichHandler
from .generator import LookmlGenerator

logging.basicConfig(
    level=getattr(logging, 'INFO'), format="%(message)s", handlers=[RichHandler()]
)

MANIFEST_PATH = './manifest.json'
DEFAULT_LOOKML_OUTPUT_DIR = '.'

def init_argparser():
    """Create and configure the argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--version',
        action='version',
        version=f'dbt2looker {version("dbt2looker_bigquery")}',
    )
    parser.add_argument(
        '--target-dir',
        help='Path to dbt target directory containing manifest.json and catalog.json. Default is "./target"',
        default='./target',
        type=str,
    )
    parser.add_argument(
        '--tag',
        help='Filter to dbt models using this tag',
        type=str,
    )
    parser.add_argument(
        '--log-level',
        help='Set level of logs. Default is INFO',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
        type=str,
        default='INFO',
    )
    parser.add_argument(
        '--output-dir',
        help='Path to a directory that will contain the generated lookml files',
        default=DEFAULT_LOOKML_OUTPUT_DIR,
        type=str,
    )
    parser.add_argument(
        '--model-connection',
        help='DB Connection Name for generated model files',
        type=str,
    )
    parser.add_argument(
        '--remove-schema-string',
        help='string to remove from folder name when generating lookml files',
        type=str,
    )
    parser.add_argument(
        '--exposures-only',
        help='add this flag to only generate lookml files for exposures',
        action='store_true',
    )
    parser.add_argument(
        '--exposures-tag',
        help='add this flag to only generate lookml files for specific tag in exposures',
        type=str,
    )
    parser.add_argument(
        '--skip-explore-joins',
        help='add this flag to skip generating an sample "explore" in views for nested structures',
        action='store_true',
    )
    parser.add_argument(
        '--use-table-name',
        help='add this flag to use table names on views and explore',
        action='store_true',
    )
    parser.add_argument(
        '--select',
        help='select a specific model to generate lookml for',
        type=str
    )
    return parser

def generate(args):
    """Run the CLI with the provided arguments"""
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logging.info('DBT2LOOKER-BIGQUERY')

    lookml_views = LookmlGenerator().generate(
        target_dir=args.target_dir,
        output_dir=args.output_dir,
        tag=args.tag,
        exposures_only=args.exposures_only,
        exposures_tag=args.exposures_tag,
        skip_explore_joins=args.skip_explore_joins,
        select=args.select,
        use_table_name_as_view=args.use_table_name
    )

    logging.info(f'Generated {len(lookml_views)} views')
    logging.info('Success')
 
 
def main():
    """Main entry point for the CLI"""
    argparser = init_argparser()
    args = argparser.parse_args()
    generate(args)

if __name__ == '__main__':
    main()
