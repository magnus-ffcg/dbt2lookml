import argparse
import logging
import os

import yaml

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version
try:
    import lkml
except ImportError:
    lkml = None
from typing import Any, Dict

from rich.logging import RichHandler

from dbt2lookml.exceptions import CliError
from dbt2lookml.generators import LookmlGenerator
from dbt2lookml.parsers import DbtParser
from dbt2lookml.utils import FileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)


class Cli:
    """Command line interface for dbt2lookml."""

    DEFAULT_LOOKML_OUTPUT_DIR = "."
    HEADER = """
    _ _   ___ _         _         _
  _| | |_|_  | |___ ___| |_ _____| |
 | . | . |  _| | . | . | '_|     | |
 |___|___|___|_|___|___|_,_|_|_|_|_|
    Convert your dbt models to LookML views
    """

    def __init__(self):
        """Initialize CLI with argument parser and file handler."""
        self._args_parser = self._init_argparser()
        self._file_handler = FileHandler()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                if config is None:
                    return {}
                return config
        except FileNotFoundError:
            raise CliError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise CliError(f"Error parsing YAML configuration: {str(e)}")

    def _merge_config_with_args(
        self, args: argparse.Namespace, config: Dict[str, Any]
    ) -> argparse.Namespace:
        """Merge configuration file values with CLI arguments. CLI args take precedence."""
        # Create a new namespace to avoid modifying the original
        merged_args = argparse.Namespace()
        # Copy all existing args
        for key, value in vars(args).items():
            setattr(merged_args, key, value)
        # Override with config values if CLI arg is default/None
        config_mapping = {
            'manifest_path': 'manifest_path',
            'catalog_path': 'catalog_path',
            'target_dir': 'target_dir',
            'output_dir': 'output_dir',
            'tag': 'tag',
            'log_level': 'log_level',
            'remove_schema_string': 'remove_schema_string',
            'exposures_only': 'exposures_only',
            'exposures_tag': 'exposures_tag',
            'use_table_name': 'use_table_name',
            'select': 'select',
            'generate_locale': 'generate_locale',
            'continue_on_error': 'continue_on_error',
            'include_models': 'include_models',
            'exclude_models': 'exclude_models',
            'timeframes': 'timeframes',
            'skip_explore': 'build_explore',
            'include_iso_fields': 'include_iso_fields',
        }
        for config_key, arg_key in config_mapping.items():
            if config_key in config:
                config_value = config[config_key]
                cli_value = getattr(args, arg_key, None)
                # Special handling for skip_explore -> build_explore inversion
                if config_key == 'skip_explore' and arg_key == 'build_explore':
                    config_value = not config_value  # Invert the boolean
                # Override defaults with config values, but CLI args take precedence
                # Check if CLI used default value by comparing with parser defaults
                is_default = False
                if arg_key == 'output_dir' and cli_value == '.':
                    is_default = True
                elif arg_key == 'target_dir' and cli_value == '.':
                    is_default = True
                elif arg_key == 'log_level' and cli_value == 'INFO':
                    is_default = True
                elif arg_key == 'build_explore' and cli_value is True:
                    is_default = True
                elif arg_key == 'include_iso_fields' and cli_value is True:
                    is_default = True
                elif cli_value is None:
                    is_default = True
                if is_default:
                    setattr(merged_args, arg_key, config_value)
                else:
                    # CLI argument was explicitly provided, keep it
                    pass
        return merged_args

    def _init_argparser(self) -> argparse.ArgumentParser:
        """Create and configure the argument parser."""
        parser = argparse.ArgumentParser(
            description=self.HEADER,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            '--version',
            action='version',
            version=f'dbt2lookml {version("dbt2lookml")}',
        )
        parser.add_argument(
            '--manifest-path',
            help='Path to dbt manifest.json file',
            default=None,
            type=str,
        )
        parser.add_argument(
            '--catalog-path',
            help='Path to dbt catalog.json file',
            default=None,
            type=str,
        )
        parser.add_argument(
            '--target-dir',
            help='Directory to output LookML files',
            default=self.DEFAULT_LOOKML_OUTPUT_DIR,
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
            help='add this flag to skip generating an sample "explore" in views for "'
            'nested structures',
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
            '--include-iso-fields',
            help='include ISO year and week fields in date dimension groups',
            action='store_true',
            default=False,
        )
        parser.add_argument(
            '--generate-locale',
            help='Generate locale files for each label on each field in view',
            action='store_true',
        )
        parser.add_argument(
            '--continue-on-error',
            help='Continue generating views even if an error occurs',
            action='store_true',
        )
        parser.add_argument(
            "--include-models",
            help="List of models to include",
            nargs="+",
            default=None,
            type=str,
        )
        parser.add_argument(
            "--exclude-models",
            help="List of models to exclude",
            nargs='+',
            default=None,
            type=str,
        )
        parser.add_argument(
            '--config',
            help='Path to YAML configuration file',
            type=str,
            default=None,
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
        try:
            # Create directory structure
            file_name = os.path.basename(file_path)
            file_path = os.path.join(output_dir, file_path.split(file_name)[0]).strip('/')
            os.makedirs(file_path, exist_ok=True)
            file_path = f'{file_path}/{file_name}'
            # Write contents
            self._file_handler.write(file_path, contents)
            logging.debug(f'Generated {file_path}')
            return file_path
        except OSError as e:
            logging.error(f"Failed to write file {file_path}: {str(e)}")
            raise CliError(f"Failed to write file {file_path}: {str(e)}") from e
        except Exception as e:
            logging.error(f"Unexpected error writing file {file_path}: {str(e)}")
            raise CliError(f"Unexpected error writing file {file_path}: {str(e)}") from e

    def generate(self, args, models):
        """Generate LookML views from dbt models"""
        if not models:
            logging.warning("No models found to process")
            return []
        logging.info('Parsing dbt models (bigquery) and creating lookml views...')
        lookml_generator = LookmlGenerator(args)
        views = []
        for model in models:
            try:
                file_path, lookml = lookml_generator.generate(
                    model=model,
                )
                view = self._write_lookml_file(
                    output_dir=args.output_dir,
                    file_path=file_path,
                    contents=lkml.dump(lookml),
                )
                views.append(view)
            except Exception as e:
                logging.error(f"Failed to generate view for model {model.name}: {str(e)}")
                if not args.continue_on_error:
                    raise
        logging.info(f'Generated {len(views)} views')
        logging.info('Success')
        return views

    def parse(self, args):
        """parse dbt models"""
        try:
            manifest: Dict = self._file_handler.read(os.path.join(args.target_dir, 'manifest.json'))
            catalog: Dict = self._file_handler.read(os.path.join(args.target_dir, 'catalog.json'))
            parser = DbtParser(args, manifest, catalog)
            return parser.get_models()
        except FileNotFoundError as e:
            raise CliError(f"Failed to read file: {str(e)}") from e
        except Exception as e:
            raise CliError(f"Unexpected error parsing dbt models: {str(e)}") from e

    def run(self):
        """Run the CLI"""
        try:
            args = self._args_parser.parse_args()
            # Load and merge configuration if provided
            if args.config:
                config = self._load_config(args.config)
                args = self._merge_config_with_args(args, config)
                logging.info(f"Loaded configuration from: {args.config}")
            logging.getLogger().setLevel(args.log_level)
            models = self.parse(args)
            self.generate(args, models)
        except CliError as e:
            # Logs should already be printed by the handler
            logging.error(f'Error occurred during generation. {str(e)}')


def main():
    cli = Cli()
    cli.run()


if __name__ == '__main__':
    main()
