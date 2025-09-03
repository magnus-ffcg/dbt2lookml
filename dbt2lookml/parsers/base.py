"""Base DBT parser functionality."""

from typing import Dict, List

from dbt2lookml.models.dbt import DbtCatalog, DbtManifest, DbtModel
from dbt2lookml.parsers.catalog import CatalogParser
from dbt2lookml.parsers.exposure import ExposureParser
from dbt2lookml.parsers.model import ModelParser


class DbtParser:
    """Main DBT parser that coordinates parsing of manifest and catalog files."""

    def __init__(self, cli_args, raw_manifest: Dict, raw_catalog: Dict):
        """Initialize the parser with raw manifest and catalog data."""
        self._cli_args = cli_args
        self._raw_manifest = raw_manifest  # Store raw manifest for metadata extraction
        self._catalog = DbtCatalog(**raw_catalog)
        self._manifest = DbtManifest(**raw_manifest)
        self._model_parser = ModelParser(self._manifest)
        self._catalog_parser = CatalogParser(self._catalog, raw_catalog)
        self._exposure_parser = ExposureParser(self._manifest)

    def get_models(self) -> List[DbtModel]:
        """Parse dbt models from manifest and filter by criteria."""
        # Get all models
        all_models = self._model_parser.get_all_models()
        # Get exposed models if needed
        exposed_names = None
        if (
            hasattr(self._cli_args, 'exposures_only')
            and self._cli_args.exposures_only
            or hasattr(self._cli_args, 'exposures_tag')
            and self._cli_args.exposures_tag
        ):
            exposed_names = self._exposure_parser.get_exposures(self._cli_args.exposures_tag)
        # Filter models based on criteria
        filtered_models = self._model_parser.filter_models(
            all_models,
            select_model=self._cli_args.select if hasattr(self._cli_args, 'select') else None,
            tag=self._cli_args.tag if hasattr(self._cli_args, 'tag') else None,
            exposed_names=exposed_names,
            include_models=getattr(self._cli_args, 'include_models', None),
            exclude_models=getattr(self._cli_args, 'exclude_models', None),
        )
        # Process models (update with catalog info)
        processed_models = []
        failed_models = []
        for model in filtered_models:
            if processed_model := self._catalog_parser.process_model_columns(model):
                # Store catalog data reference for generators
                processed_model._catalog_data = self._catalog_parser._raw_catalog_data
                # Store original raw manifest data for metadata extraction
                # Use the raw manifest dict passed to constructor, not the parsed Pydantic model
                raw_nodes = self._raw_manifest.get('nodes', {})
                if model.unique_id in raw_nodes:
                    processed_model._manifest_data = raw_nodes[model.unique_id]
                processed_models.append(processed_model)
            else:
                failed_models.append(model.name)

        # Log any models that failed processing
        if failed_models:
            import logging

            logging.warning(
                f"Failed to process {len(failed_models)} models during catalog parsing: {', '.join(failed_models[:5])}{'...' if len(failed_models) > 5 else ''}"
            )

        return processed_models
