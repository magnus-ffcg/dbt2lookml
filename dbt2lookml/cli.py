import argparse
import os
import lkml

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

from dbt2lookml.generators import LookmlGenerator
from dbt2lookml.enums import LookerScalarTypes
from dbt2lookml.exceptions import CliError
from dbt2lookml.utils import FileHandler
from dbt2lookml.parsers import DbtParser

import logging
from rich.logging import RichHandler

logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)


class Cli:
    DEFAULT_LOOKML_OUTPUT_DIR = '.'
    HEADER = """
    _ _   ___ _         _         _ 
  _| | |_|_  | |___ ___| |_ _____| |
 | . | . |  _| | . | . | '_|     | |
 |___|___|___|_|___|___|_,_|_|_|_|_|

    Convert your dbt models to LookML views   
                                                                               
    """

    def __init__(self):
        self._args_parser = self._init_argparser()
        self._file_handler = FileHandler()

    def _init_argparser(self):
        """Create and configure the argument parser"""
        parser = argparse.ArgumentParser(
            description=self.HEADER, formatter_class=argparse.RawDescriptionHelpFormatter
        )
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
            '--skip-explore',
            help='add this flag to skip generating an sample "explore" in views for nested structures',
            action='store_false',
            dest="build_explore",
        )
        parser.add_argument(
            '--use-table-name',
            help='add this flag to use table names on views and explore',
            action='store_true',
        )
        parser.add_argument(
            '--select', help='select a specific model to generate lookml for', type=str
        )
        parser.add_argument(
            '--generate-locale',
            help='Generate locale files for each label on each field in view',
            action='store_true',
        )
        parser.set_defaults(build_explore=True)
        return parser

    def _write_lookml_file(
        self,
        output_dir: str,
        file_path: str,
        contents: str,
    ) -> str:
        """Write LookML content to a file."""
        # Create directory structure

        file_name = os.path.basename(file_path)
        file_path = os.path.join(output_dir, file_path.split(file_name)[0])
        os.makedirs(file_path, exist_ok=True)

        file_path = f'{file_path}/{file_name}'

        # Write contents
        self._file_handler.write(file_path, contents)
        logging.debug(f'Generated {file_path}')

        return file_path

    def generate(self, args, models):
        """Generate LookML views from dbt models"""
        logging.info('Parsing dbt models (bigquery) and creating lookml views...')

        lookml_generator = LookmlGenerator(args)

        views = []
        for model in models:
            file_path, lookml = lookml_generator.generate(
                model=model,
            )

            view = self._write_lookml_file(
                output_dir=args.output_dir,
                file_path=file_path,
                contents=lkml.dump(lookml),
            )

            views.append(view)

        logging.info(f'Generated {len(views)} views')
        logging.info('Success')

    def parse(self, args):
        """parse dbt models"""
        raw_manifest = self._file_handler.read(os.path.join(args.target_dir, 'manifest.json'))
        raw_catalog = self._file_handler.read(os.path.join(args.target_dir, 'catalog.json'))

        parser = DbtParser(raw_manifest, raw_catalog)
        return parser.get_models(args)

    def run(self):
        """Run the CLI"""
        try:
            args = self._args_parser.parse_args()
            logging.getLogger().setLevel(args.log_level)

            models = self.parse(args)
            self.generate(args, models)

        except CliError as e:
            # Logs should already be printed by the handler
            logging.error('Error occurred during generation. Stopped execution.')


def main():
    cli = Cli()
    cli.run()


if __name__ == '__main__':
    main()
