"""Model-specific parsing functionality."""

import logging
from typing import Dict, List, Optional

from dbt2lookml.models.dbt import DbtModel, DbtManifest


class ModelParser:
    """Parser for DBT models from manifest."""

    def __init__(self, manifest: DbtManifest):
        """Initialize with manifest data."""
        self._manifest = manifest

    def get_all_models(self) -> List[DbtModel]:
        """Get all models from manifest."""
        all_models = self._filter_nodes_by_type(self._manifest.nodes, 'model')

        for model in all_models:
            if not hasattr(model, 'name'):
                logging.error(
                    'Cannot parse model with id: "%s" - is the model file empty?', model.unique_id
                )
                continue

        return all_models

    def filter_models(
        self,
        models_list: List[DbtModel],
        select_model: Optional[str] = None,
        tag: Optional[str] = None,
        exposed_names: Optional[List[str]] = None,
    ) -> List[DbtModel]:
        """Filter models based on multiple criteria."""
        filtered = models_list

        if select_model:
            return [model for model in filtered if model.name == select_model]

        if tag:
            filtered = [model for model in filtered if self._tags_match(tag, model)]

        if exposed_names:
            filtered = [model for model in filtered if model.name in exposed_names]

        return filtered

    def _filter_nodes_by_type(self, nodes: Dict, resource_type: str) -> List[DbtModel]:
        """Filter nodes by resource type and ensure they have names."""
        return [
            node
            for node in nodes.values()
            if isinstance(node, DbtModel) and node.resource_type == resource_type
        ]

    def _tags_match(self, tag: str, model: DbtModel) -> bool:
        """Check if model has the specified tag."""
        return tag in model.tags
