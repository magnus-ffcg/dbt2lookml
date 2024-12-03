import logging
from typing import Dict, Optional, List
from dbt2looker_bigquery import models as models


class DbtParser():
    def __init__(self, raw_manifest, raw_catalog):
        self.catalog = models.DbtCatalog(**raw_catalog)
        self.manifest = models.DbtManifest(**raw_manifest)

    def _tags_match(self, query_tag: str, model: models.DbtModel) -> bool:
        """Check if a model has a specific tag"""
        try:
            return query_tag in model.tags
        except (AttributeError, ValueError):
            return False

    def _get_models_by_criteria(self, all_models: List[models.DbtModel], tag: Optional[str], 
                              exposed_model_names: Optional[List[str]], select_model: Optional[str]) -> List[models.DbtModel]:
        """Filter models based on given criteria"""
        if select_model:
            return [model for model in all_models if model.name == select_model]
        
        filtered_models = all_models
        
        if tag:
            filtered_models = [model for model in filtered_models if self._tags_match(tag, model)]
        
        if exposed_model_names:
            filtered_models = [model for model in filtered_models if model.name in exposed_model_names]
        
        return filtered_models

    def _get_exposed_models(self, exposures_tag: Optional[str] = None) -> List[str]:
        """Get list of exposed model names, optionally filtered by tag"""
        exposures = [
            node for node in self.manifest.exposures.values()
            if node.resource_type == 'exposure' and hasattr(node, 'name')
        ]
        
        if exposures_tag:
            exposures = [exp for exp in exposures if self._tags_match(exposures_tag, exp)]
        
        exposed_models = []
        for exposure in exposures:
            exposed_models.extend(ref.name for ref in exposure.refs)
        
        return list(set(exposed_models))

    def parse_models(self, tag: Optional[str] = None, exposures_only: bool = False, 
                    exposures_tag: Optional[str] = None, select_model: Optional[str] = None) -> List[models.DbtModel]:
        """Parse dbt models from manifest and filter by criteria"""
        all_models = [
            node for node in self.manifest.nodes.values()
            if node.resource_type == 'model' and hasattr(node, 'name')
        ]

        for model in all_models:
            if not hasattr(model, 'name'):
                logging.error('Cannot parse model with id: "%s" - is the model file empty?', model.unique_id)
                raise SystemExit('Failed')

        exposed_model_names = self._get_exposed_models(exposures_tag) if exposures_only else None
        return self._get_models_by_criteria(all_models, tag, exposed_model_names, select_model)

    def _get_catalog_column_info(self, model_id: str, column_name: str) -> tuple:
        """Get column type information from catalog"""
        node = self.catalog.nodes.get(model_id)
        if not node:
            return None, []
        
        column = node.columns.get(column_name.lower())
        if not column:
            return None, []
        
        return column.data_type, column.inner_types

    def _create_missing_array_column(self, column_name: str, data_type: str, inner_types: List[str]) -> models.DbtModelColumn:
        """Create a new column model for array columns missing from manifest"""
        return models.DbtModelColumn(
            name=column_name,
            description="missing column from manifest.json, generated from catalog.json",
            data_type=data_type,
            inner_types=inner_types,
            meta=models.DbtModelColumnMeta()
        )

    def parse_typed_models(self, tag: Optional[str] = None, exposures_only: bool = False, 
                          exposures_tag: Optional[str] = None, select_model: Optional[str] = None) -> List[models.DbtModel]:
        """Parse models and enrich them with type information from catalog"""
        dbt_models = self.parse_models(tag=tag, exposures_only=exposures_only, 
                                     exposures_tag=exposures_tag, select_model=select_model)
        adapter_type = self.manifest.metadata.adapter_type
        
        # Log model materialization status
        logging.debug('Found manifest entries for %d models', len(dbt_models))
        for model in dbt_models:
            if hasattr(model, 'columns') and model.columns:
                measure_count = sum(len(col.meta.looker_measures) for col in model.columns.values())
                logging.debug('Model %s has %d columns with %d measures',
                            model.name, len(model.columns), measure_count)
                
                if model.unique_id not in self.catalog.nodes:
                    logging.debug(
                        f'Model {model.unique_id} not found in catalog. No looker view will be generated. '
                        f'Check if model has materialized in {adapter_type} at {model.relation_name}'
                    )

        # Update models with catalog information
        dbt_typed_models = []
        for model in dbt_models:
            if model.unique_id not in self.catalog.nodes:
                continue

            # Update existing columns with types
            updated_columns = {}
            for column in model.columns.values():
                data_type, inner_types = self._get_catalog_column_info(model.unique_id, column.name)
                            
                updated_columns[column.name] = column.model_copy(update={
                    'data_type': data_type,
                    'inner_types': inner_types
                })

            # Add missing array columns from catalog
            catalog_node = self.catalog.nodes[model.unique_id]
            for col_name, catalog_col in catalog_node.columns.items():
                if col_name not in model.columns and catalog_col.type.startswith('ARRAY'):
                    logging.debug("%s is an array column", col_name)
                    updated_columns[col_name] = self._create_missing_array_column(
                        col_name, catalog_col.data_type, catalog_col.inner_types
                    )

            typed_model = model.model_copy(update={'columns': updated_columns})
            dbt_typed_models.append(typed_model)

        logging.debug('Found catalog entries for %d models', len(dbt_typed_models))
        logging.debug('Catalog entries missing for %d models', len(dbt_models) - len(dbt_typed_models))

        # Check for models with missing column types
        for model in dbt_typed_models:
            if all(col.data_type is None for col in model.columns.values()):
                logging.debug('Model %s has no typed columns, no dimensions will be generated. %s', 
                            model.unique_id, model)

        return dbt_typed_models