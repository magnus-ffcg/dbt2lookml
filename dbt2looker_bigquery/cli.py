import argparse
import json
import logging
from rich.logging import RichHandler

import pathlib
import os
try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

from . import parser
from . import generator

MANIFEST_PATH = './manifest.json'
DEFAULT_LOOKML_OUTPUT_DIR = '.'

def get_manifest(prefix: str):
    manifest_path = os.path.join(prefix, 'manifest.json')
    try:
        with open(manifest_path, 'r') as f:
            raw_manifest = json.load(f)
    except FileNotFoundError as e:
        logging.error(f'Could not find manifest file at {manifest_path}. Use --target-dir to change the search path for the manifest.json file.')
        raise SystemExit('Failed')
    logging.debug(f'Detected manifest at {manifest_path}')
    return raw_manifest


def get_catalog(prefix: str):
    catalog_path = os.path.join(prefix, 'catalog.json')
    try:
        with open(catalog_path, 'r') as f:
            raw_catalog = json.load(f)
    except FileNotFoundError as e:
        logging.error(f'Could not find catalog file at {catalog_path}. Use --target-dir to change the search path for the catalog.json file.')
        raise SystemExit('Failed')
    logging.debug(f'Detected catalog at {catalog_path}')
    return raw_catalog

def run():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        '--version',
        action='version',
        version=f'dbt2looker {version("dbt2looker_bigquery")}',
    )
    argparser.add_argument(
        '--target-dir',
        help='Path to dbt target directory containing manifest.json and catalog.json. Default is "./target"',
        default='./target',
        type=str,
    )
    argparser.add_argument(
        '--tag',
        help='Filter to dbt models using this tag',
        type=str,
    )
    argparser.add_argument(
        '--log-level',
        help='Set level of logs. Default is INFO',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
        type=str,
        default='INFO',
    )
    argparser.add_argument(
        '--output-dir',
        help='Path to a directory that will contain the generated lookml files',
        default=DEFAULT_LOOKML_OUTPUT_DIR,
        type=str,
    )
    argparser.add_argument(
        '--model-connection',
        help='DB Connection Name for generated model files',
        type=str,
    )
    argparser.add_argument(
        '--remove-schema-string',
        help='string to remove from folder name when generating lookml files',
        type=str,
    )
    argparser.add_argument(
        '--exposures-only',
        help='add this flag to only generate lookml files for exposures',
        action='store_true',  # This makes the flag a boolean argument
    )
    argparser.add_argument(
        '--exposures-tag',
        help='add this flag to only generate lookml files for specific tag in exposures',
        type=str,  
    )
    argparser.add_argument(
        '--skip-explore-joins',
        help='add this flag to skip generating an sample "explore" in views for nested structures',
        action='store_true',  
    )
    argparser.add_argument(
        '--use-table-name',
        help='add this flag to use table names on views and explore',
        action='store_true',  
    )
    argparser.add_argument(
        '--select',
        help='select a specific model to generate lookml for',
        type=str
    )
    args = argparser.parse_args()
    FORMAT = "%(message)s"
    logging.basicConfig(
        level=getattr(logging, args.log_level), format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
    )

    # Load manifest and catalog once.
    raw_manifest = get_manifest(prefix=args.target_dir)
    manifest = parser.parse_manifest(raw_manifest)
    raw_catalog = get_catalog(prefix=args.target_dir)
    catalog = parser.parse_catalog(raw_catalog)

    # Get dbt models from manifest
    typed_dbt_models = parser.parse_typed_models(manifest, catalog, tag=args.tag, exposures_only=args.exposures_only, exposures_tag=args.exposures_tag, select_model=args.select)

    # Generate lookml views
    lookml_views = [
        generator.lookml_view_from_dbt_model(model, args.output_dir, args.skip_explore_joins, args.use_table_name)
        for model in typed_dbt_models
    ]
    
    logging.info('Generated %s views', len(lookml_views))
    
    logging.info('Success')
 