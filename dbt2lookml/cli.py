import argparse
import json
import pathlib
import os

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

from dbt2lookml.generators.lookml import LookmlGenerator
from dbt2lookml.log import Logger
from dbt2lookml.enums import LookerScalarTypes
from dbt2lookml.exceptions import CliError

class Cli(Logger):
    DEFAULT_LOOKML_OUTPUT_DIR = '.'
    
    def _init_argparser(self):
        """Create and configure the argument parser"""
        parser = argparse.ArgumentParser(description="dbt2lookml - Generate Looker views from dbt models (bigquery)")
        parser.add_argument(
            '--version',
            action='version',
            version=f'dbt2lookml {version("dbt2lookml")}',
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
            default=self.DEFAULT_LOOKML_OUTPUT_DIR,
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
        parser.add_argument(
            '--generate-locale',
            help='Generate locale files for each label on each field in view',
            action='store_true'
        )
        return parser

    def generate(self,args):
        """Run the CLI with the provided arguments"""
        
        self._logger.info('DBT2LOOKML - Generate Looker views from dbt models (bigquery)')
        
        lookml_views = LookmlGenerator().generate(
            target_dir=args.target_dir,
            output_dir=args.output_dir,
            tag=args.tag,
            exposures_only=args.exposures_only,
            exposures_tag=args.exposures_tag,
            skip_explore_joins=args.skip_explore_joins,
            select=args.select,
            use_table_name_as_view=args.use_table_name,
            generate_locale=args.generate_locale
        )

        self._logger.info(f'Generated {len(lookml_views)} views')
        self._logger.info('Success')
 
    def run(self):
        """Run the CLI"""
        try:
            argparser = self._init_argparser()
            args = argparser.parse_args()
            
            self.set_loglevel(args.log_level)
            self.generate(args)
            
        except CliError as e:
            # Logs should already be printed by the handler 
            self._logger.error('Error occurred during generation. Stopped execution.')


def main():
    cli = Cli()
    cli.run()   

if __name__ == '__main__':
    main()
    
