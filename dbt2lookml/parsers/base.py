"""Base DBT parser functionality."""

import logging
from typing import Dict, List, Optional, Tuple

from dbt2lookml.models.dbt import DbtModel, DbtCatalog, DbtManifest, DbtModelColumn
from dbt2lookml.parsers.model import ModelParser
from dbt2lookml.parsers.catalog import CatalogParser
from dbt2lookml.parsers.exposure import ExposureParser


class DbtParser:
    """Main DBT parser that coordinates parsing of manifest and catalog files."""

    def __init__(self, raw_manifest: Dict, raw_catalog: Dict):
        """Initialize the parser with raw manifest and catalog data."""
        self._catalog = DbtCatalog(**raw_catalog)
        self._manifest = DbtManifest(**raw_manifest)
        self._model_parser = ModelParser(self._manifest)
        self._catalog_parser = CatalogParser(self._catalog)
        self._exposure_parser = ExposureParser(self._manifest)

    def get_models(self, args) -> List[DbtModel]:
        """Parse dbt models from manifest and filter by criteria."""
        # Get all models
        all_models = self._model_parser.get_all_models()

        # Get exposed models if needed
        exposed_names = None
        if (
            hasattr(args, 'exposures_only')
            and args.exposures_only
            or hasattr(args, 'exposures_tag')
            and args.exposures_tag
            or hasattr(args, 'build_explore')
            and args.build_explore
        ):
            exposed_names = self._exposure_parser.get_exposures(
                args.exposures_tag if hasattr(args, 'exposures_tag') else None
            )

        # Filter models based on criteria
        filtered_models = self._model_parser.filter_models(
            all_models,
            select_model=args.select if hasattr(args, 'select') else None,
            tag=args.tag if hasattr(args, 'tag') else None,
            exposed_names=exposed_names,
        )

        # Process models (update with catalog info)
        processed_models = []
        for model in filtered_models:
            if processed_model := self._catalog_parser.process_model(model):
                processed_models.append(processed_model)

        return processed_models
