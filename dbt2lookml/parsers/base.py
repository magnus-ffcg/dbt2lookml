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
        self._catalog = DbtCatalog(**raw_catalog)
        self._manifest = DbtManifest(**raw_manifest)
        self._model_parser = ModelParser(self._manifest)
        self._catalog_parser = CatalogParser(self._catalog)
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
        )

        # Process models (update with catalog info)
        processed_models = []
        for model in filtered_models:
            if processed_model := self._catalog_parser.process_model_columns(model):
                processed_models.append(processed_model)

        return processed_models
