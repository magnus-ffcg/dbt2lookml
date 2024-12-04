import logging
from typing import Dict, Optional, List
from dbt2looker_bigquery import models


class DbtParser():
    def __init__(self, raw_manifest, raw_catalog):
        self.catalog = models.DbtCatalog(**raw_catalog)
        self.manifest = models.DbtManifest(**raw_manifest)
        self.logger = logging.getLogger("rich")

    def _filter_nodes_by_type(self, nodes: Dict, resource_type: str) -> List:
        """Filter nodes by resource type and ensure they have names"""
        return [
            node for node in nodes.values()
            if node.resource_type == resource_type and hasattr(node, 'name')
        ]

    def _tags_match(self, query_tag: str, model: models.DbtModel) -> bool:
        """Check if a model has a specific tag"""
        try:
            return query_tag in model.tags
        except (AttributeError, ValueError):
            return False

    def _filter_models(self, models_list: List[models.DbtModel], **filters) -> List[models.DbtModel]:
        """Filter models based on multiple criteria"""
        filtered = models_list

        if filters.get('select_model'):
            return [model for model in filtered if model.name == filters['select_model']]
        
        if filters.get('tag'):
            filtered = [model for model in filtered if self._tags_match(filters['tag'], model)]
        
        if filters.get('exposed_names'):
            filtered = [model for model in filtered if model.name in filters['exposed_names']]
        
        return filtered

    def _get_exposed_models(self, exposures_tag: Optional[str] = None) -> List[str]:
        """Get list of exposed model names, optionally filtered by tag"""
        exposures = self._filter_nodes_by_type(self.manifest.exposures, 'exposure')
        
        if exposures_tag:
            exposures = [exp for exp in exposures if self._tags_match(exposures_tag, exp)]
        
        return list(set(
            ref.name for exposure in exposures
            for ref in exposure.refs
        ))

    def _update_column_with_types(self, column: models.DbtModelColumn, model_id: str) -> models.DbtModelColumn:
        """Update a column with type information from catalog"""
        data_type, inner_types = self._get_catalog_column_info(model_id, column.name)
        return column.model_copy(update={
            'data_type': data_type,
            'inner_types': inner_types
        })

    def _create_missing_array_column(self, column_name: str, data_type: str, inner_types: List[str]) -> models.DbtModelColumn:
        """Create a new column model for array columns missing from manifest"""
        return models.DbtModelColumn(
            name=column_name,
            description="missing column from manifest.json, generated from catalog.json",
            data_type=data_type,
            inner_types=inner_types,
            meta=models.DbtModelColumnMeta()
        )

    def _get_catalog_column_info(self, model_id: str, column_name: str) -> tuple:
        """Get column type information from catalog"""
        node = self.catalog.nodes.get(model_id)
        if not node or column_name.lower() not in node.columns:
            return None, []
        
        column = node.columns[column_name.lower()]
        return column.data_type, column.inner_types

    def parse_models(self, tag: Optional[str] = None, exposures_only: bool = False, 
                    exposures_tag: Optional[str] = None, select_model: Optional[str] = None) -> List[models.DbtModel]:
        """Parse dbt models from manifest and filter by criteria"""
        all_models = self._filter_nodes_by_type(self.manifest.nodes, 'model')

        for model in all_models:
            if not hasattr(model, 'name'):
                self.logger.error('Cannot parse model with id: "%s" - is the model file empty?', model.unique_id)
                raise SystemExit('Failed')

        exposed_names = self._get_exposed_models(exposures_tag) if exposures_only else None
        return self._filter_models(all_models, tag=tag, exposed_names=exposed_names, select_model=select_model)

    def parse_typed_models(self, tag: Optional[str] = None, exposures_only: bool = False, 
                          exposures_tag: Optional[str] = None, select_model: Optional[str] = None) -> List[models.DbtModel]:
        """Parse models and enrich them with type information from catalog"""
        dbt_models = self.parse_models(tag=tag, exposures_only=exposures_only, 
                                     exposures_tag=exposures_tag, select_model=select_model)
        
        self._log_model_stats(dbt_models)
        return self._process_typed_models(dbt_models)

    def _log_model_stats(self, models: List[models.DbtModel]) -> None:
        """Log statistics about the models"""
        self.logger.debug('Found manifest entries for %d models', len(models))
        adapter_type = self.manifest.metadata.adapter_type
        
        for model in models:
            if not hasattr(model, 'columns') or not model.columns:
                continue
                
            measure_count = sum(len(col.meta.looker_measures) for col in model.columns.values())
            self.logger.debug('Model %s has %d columns with %d measures',
                          model.name, len(model.columns), measure_count)
            
            if model.unique_id not in self.catalog.nodes:
                self.logger.debug(
                    f'Model {model.unique_id} not found in catalog. No looker view will be generated. '
                    f'Check if model has materialized in {adapter_type} at {model.relation_name}'
                )

    def _process_typed_models(self, models: List[models.DbtModel]) -> List[models.DbtModel]:
        """Process models and add type information from catalog"""
        typed_models = []
        
        for model in models:
            if model.unique_id not in self.catalog.nodes:
                continue

            updated_columns = {
                col.name: self._update_column_with_types(col, model.unique_id)
                for col in model.columns.values()
            }

            # Add missing array columns from catalog
            catalog_node = self.catalog.nodes[model.unique_id]
            for col_name, catalog_col in catalog_node.columns.items():
                if col_name not in model.columns and catalog_col.type.startswith('ARRAY'):
                    self.logger.debug("%s is an array column", col_name)
                    updated_columns[col_name] = self._create_missing_array_column(
                        col_name, catalog_col.data_type, catalog_col.inner_types
                    )

            typed_model = model.model_copy(update={'columns': updated_columns})
            typed_models.append(typed_model)

        self._log_typed_model_stats(models, typed_models)
        return typed_models

    def _log_typed_model_stats(self, original_models: List[models.DbtModel], typed_models: List[models.DbtModel]) -> None:
        """Log statistics about typed models"""
        self.logger.debug('Found catalog entries for %d models', len(typed_models))
        self.logger.debug('Catalog entries missing for %d models', len(original_models) - len(typed_models))

        for model in typed_models:
            if all(col.data_type is None for col in model.columns.values()):
                self.logger.debug('Model %s has no typed columns, no dimensions will be generated. %s', 
                              model.unique_id, model)