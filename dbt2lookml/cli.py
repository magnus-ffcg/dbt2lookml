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

    def _get_config_with_defaults(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            'manifest_path': None,
            'catalog_path': None,
            'target_dir': '.',
            'output_dir': '.',
            'tag': None,
            'log_level': 'INFO',
            'remove_schema_string': None,
            'exposures_only': False,
            'exposures_tag': None,
            'use_table_name': False,
            'select': None,
            'generate_locale': False,
            'continue_on_error': False,
            'include_models': None,
            'exclude_models': None,
            'timeframes': None,
            'include_iso_fields': False,
        }

    def _merge_config_with_args(
        self, args: argparse.Namespace, config_file: Dict[str, Any]
    ) -> argparse.Namespace:
        """Merge configuration: defaults < config file < CLI args."""
        # Start with defaults
        config = self._get_config_with_defaults()
        
        # Override with config file values
        for key, value in config_file.items():
            if key in config:
                config[key] = value

        # Override with CLI args (only non-default values)
        parser_defaults = self._get_config_with_defaults()
        for key, value in vars(args).items():
            # If CLI value differs from default, use CLI value
            if key in parser_defaults and value != parser_defaults[key]:
                config[key] = value
            # Always use CLI value if not in defaults (new args)
            elif key not in parser_defaults:
                config[key] = value
        
        # Convert back to namespace
        return argparse.Namespace(**config)

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
        parser.add_argument(
            '--validate',
            help='Validate generated LookML files for syntax errors',
            action='store_true',
        )
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
        """Generate LookML views from dbt models using concurrent processing"""
        if not models:
            logging.warning("No models found to process")
            return []
        logging.info('Parsing dbt models (bigquery) and creating lookml views...')
        
        views = []
        failed_count = 0
        validation_failed_count = 0
        written_files = {}     # Track unique file paths in main thread
        duplicate_files = []   # Track duplicates
        failed_models = []     # Track which models failed
        
        # Counter for table name duplicates (only used when --use-table-name is set)
        table_name_counter = {} if args.use_table_name else None
        
        # Process models sequentially
        for i, model in enumerate(models):
            try:
                result = self._generate_single_model(args, model, table_name_counter)
                
                if result and result != 'validation_failed':
                    # Debug: Log what we're adding to written_files
                    logging.debug(f"Model {model.name} returned result: {result}")
                    # Check for duplicate file paths
                    if result in written_files:
                        duplicate_files.append((model.name, result))
                        logging.debug(f"Duplicate file path detected: {result}")
                    else:
                        written_files[result] = 1
                        logging.debug(f"Added to written_files: {result} (total: {len(written_files)})")
                    views.append(result)
                elif result == 'validation_failed':
                    validation_failed_count += 1
                    failed_models.append(f"{model.name} (validation)")
                else:
                    failed_count += 1
                    failed_models.append(f"{model.name} (generation)")
                    
            except Exception as e:
                logging.error(f"Failed to generate view for model {model.name}: {str(e)}")
                failed_count += 1
                failed_models.append(f"{model.name} (exception: {str(e)[:50]}...)")
                if not args.continue_on_error:
                    raise
        
        total_attempted = len(models)
        files_written = len(views)
        unique_files_written = len(written_files)
        files_generated = files_written + validation_failed_count
        
        # Report detailed results
        logging.info(f'Generation Results:')
        logging.info(f'  - Models to process: {total_attempted}')
        logging.info(f'  - Files written: {files_written}')
        logging.info(f'  - Unique file paths: {unique_files_written}')
        
        if duplicate_files:
            logging.warning(f'  - Duplicate file paths detected: {len(duplicate_files)}')
            logging.warning(f'    First few duplicates: {duplicate_files[:3]}')
        
        if validation_failed_count > 0:
            logging.warning(f'  - Files generated but failed validation: {validation_failed_count}')
            validation_failures = [m for m in failed_models if '(validation)' in m]
            if validation_failures:
                logging.warning(f'    Validation failures: {validation_failures[:3]}{", ..." if len(validation_failures) > 3 else ""}')
        if failed_count > 0:
            logging.warning(f'  - Files failed to generate: {failed_count}')
            logging.warning(f'    Failed models: {failed_models[:5]}{", ..." if len(failed_models) > 5 else ""}')
        
        # Calculate success rate based on unique files
        if total_attempted > 0:
            success_rate = (unique_files_written / total_attempted) * 100
            logging.info(f'  - Success rate: {success_rate:.1f}% ({unique_files_written}/{total_attempted})')
        
        # Only report success if all files were written successfully and no duplicates
        if failed_count == 0 and validation_failed_count == 0 and not duplicate_files:
            logging.info('All files generated successfully')
        elif unique_files_written > 0:
            logging.info('Generation completed with some issues')
        else:
            logging.error('Generation failed - no files were written')
        return views
    
    def _generate_single_model(self, args, model, table_name_counter=None):
        """Generate and validate LookML for a single model."""
        try:
            lookml_generator = LookmlGenerator(args)
            file_path, lookml = lookml_generator.generate(model=model)
            contents = lkml.dump(lookml)
            
            # Handle duplicate file paths when using table names
            if args.use_table_name and table_name_counter is not None:
                original_path = file_path
                counter = table_name_counter.get(original_path, 0)
                if counter > 0:
                    # Append number to file name
                    base_path, ext = os.path.splitext(file_path)
                    file_path = f"{base_path}_{counter}{ext}"
                table_name_counter[original_path] = counter + 1
            
            # Validate the generated content before writing (only if --validate flag is set)
            if args.validate:
                from dbt2lookml.validation import LookMLValidator
                validator = LookMLValidator()
                validation_result = validator.validate_lookml_string(contents, file_path)
                
                if not validation_result['valid']:
                    logging.error(f"Generated LookML for {model.name} failed validation: {validation_result['errors']}")
                    return 'validation_failed'
            
            written_file_path = self._write_lookml_file(
                output_dir=args.output_dir,
                file_path=file_path,
                contents=contents,
            )
            return written_file_path
        except Exception as e:
            logging.error(f"Failed to generate view for model {model.name}: {str(e)}")
            # Re-raise exception if continue_on_error is False, otherwise return None
            if hasattr(args, 'continue_on_error') and not args.continue_on_error:
                raise
            return None
    
    # Keep backward compatibility for tests
    def _generate_single_model_legacy(self, args, model):
        """Legacy method signature for backward compatibility with tests."""
        return self._generate_single_model(args, model, None)

    def parse(self, args):
        """parse dbt models"""
        try:
            # Use custom paths if provided, otherwise fall back to target_dir
            manifest_path = args.manifest_path if args.manifest_path else os.path.join(args.target_dir, 'manifest.json')
            catalog_path = args.catalog_path if args.catalog_path else os.path.join(args.target_dir, 'catalog.json')
            
            manifest: Dict = self._file_handler.read(manifest_path)
            catalog: Dict = self._file_handler.read(catalog_path)
            parser = DbtParser(args, manifest, catalog)
            models = parser.get_models()
            
            # Log parsing results
            total_models_in_manifest = len(manifest.get('nodes', {})) if isinstance(manifest, dict) else 0
            models_after_filtering = len(models)
            logging.info(f'Found {total_models_in_manifest} models in manifest, {models_after_filtering} models after filtering and processing')
            
            return models
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
                config_file = self._load_config(args.config)
                args = self._merge_config_with_args(args, config_file)
                logging.info(f"Loaded configuration from: {args.config}")
            logging.getLogger().setLevel(args.log_level)
            models = self.parse(args)
            if not models:
                logging.error('No models found to process. Check your filtering criteria.')
                return
            generated_views = self.generate(args, models)
            
            # Validation is now done inline during generation
                    
        except CliError as e:
            # Logs should already be printed by the handler
            logging.error(f'Error occurred during generation. {str(e)}')


def main():
    cli = Cli()
    cli.run()


if __name__ == '__main__':
    main()
