"""Catalog-specific parsing functionality."""

from typing import List, Optional, Tuple

from dbt2lookml.models.dbt import DbtCatalog, DbtModel, DbtModelColumn, DbtModelColumnMeta


class CatalogParser:
    """Parser for DBT catalog information."""

    def __init__(self, catalog: DbtCatalog, raw_catalog_data: dict = None):
        """Initialize with catalog data."""
        self._catalog = catalog
        self._raw_catalog_data = raw_catalog_data

    def process_model_columns(self, model: DbtModel) -> Optional[DbtModel]:
        """Process a model by updating its columns with catalog information."""
        processed_columns = {}
        # First, find the original case mapping from raw catalog data
        original_case_mapping = {}
        if self._raw_catalog_data and model.unique_id in self._raw_catalog_data.get('nodes', {}):
            raw_catalog_node = self._raw_catalog_data['nodes'][model.unique_id]
            for catalog_col_name in raw_catalog_node.get('columns', {}).keys():
                original_case_mapping[catalog_col_name.lower()] = catalog_col_name
        elif model.unique_id in self._catalog.nodes:
            # Fallback to processed catalog if raw data not available
            catalog_node = self._catalog.nodes[model.unique_id]
            for catalog_col_name in catalog_node.columns.keys():
                original_case_mapping[catalog_col_name.lower()] = catalog_col_name
        
        # Process existing columns and preserve original case
        for column_name, column in model.columns.items():
            processed_column = self._update_column_with_inner_types(column, model.unique_id)
            if processed_column:
                # Update original_name if we have the proper case from catalog
                if column_name in original_case_mapping:
                    processed_column.original_name = original_case_mapping[column_name]
                processed_columns[column_name] = processed_column
            else:
                # Update original_name for existing column if we have proper case
                if column_name in original_case_mapping:
                    # Create new column with updated original_name
                    updated_column = column.model_copy(update={'original_name': original_case_mapping[column_name]})
                    processed_columns[column_name] = updated_column
                else:
                    processed_columns[column_name] = column
        # Create missing array and nested struct columns using raw catalog data
        if self._raw_catalog_data and model.unique_id in self._raw_catalog_data.get('nodes', {}):
            raw_catalog_node = self._raw_catalog_data['nodes'][model.unique_id]
            for column_name, column_data in raw_catalog_node.get('columns', {}).items():
                # Check both original case and lowercase for existing columns
                if (column_name.lower() not in processed_columns and 
                    column_data.get('type') is not None):
                    # Add missing array columns
                    if 'ARRAY' in f'{column_data.get("type", "")}':
                        array_column = self._create_missing_array_column(
                            column_name.lower(), column_data.get('type'), []
                        )
                        array_column.original_name = column_name
                        processed_columns[column_name.lower()] = array_column
                    # Add nested struct fields (columns with dots in name)
                    elif '.' in column_name:
                        # Store with lowercase key but preserve original case in original_name
                        nested_column = self._create_missing_nested_column(
                            column_name.lower(), column_data.get('type'), 
                            column_data.get('comment'), column_name
                        )
                        # Ensure original_name preserves the exact case from catalog
                        nested_column.original_name = column_name
                        processed_columns[column_name.lower()] = nested_column
        elif model.unique_id in self._catalog.nodes:
            # Fallback to processed catalog if raw data not available
            catalog_node = self._catalog.nodes[model.unique_id]
            for column_name, column in catalog_node.columns.items():
                # Check both original case and lowercase for existing columns
                if (column_name not in processed_columns and 
                    column_name.lower() not in processed_columns and 
                    column.data_type is not None):
                    # Add missing array columns
                    if 'ARRAY' in f'{column.data_type}':
                        processed_columns[column_name] = self._create_missing_array_column(
                            column_name, column.data_type, column.inner_types or []
                        )
                    # Add nested struct fields (columns with dots in name)
                    elif '.' in column_name:
                        # Store with lowercase key but preserve original case in original_name
                        nested_column = self._create_missing_nested_column(
                            column_name.lower(), column.data_type, column.comment, column_name
                        )
                        # Ensure original_name preserves the exact case from catalog
                        nested_column.original_name = column_name
                        processed_columns[column_name.lower()] = nested_column
        # Always return the model, even if no columns were processed
        return (
            model.model_copy(update={'columns': processed_columns}) if processed_columns else model
        )

    def _create_missing_array_column(
        self, column_name: str, data_type: str, inner_types: List[str]
    ) -> DbtModelColumn:
        """Create a new column model for array columns missing from manifest."""
        from dbt2lookml.utils import camel_to_snake
        return DbtModelColumn(
            name=column_name,
            data_type=data_type,
            inner_types=inner_types,
            description=None,
            meta=DbtModelColumnMeta(),
            lookml_name=camel_to_snake(column_name.split('.')[-1]),
            original_name=column_name,
        )

    def _create_missing_nested_column(
        self, column_name: str, data_type: str, comment: str, original_column_name: str = None
    ) -> DbtModelColumn:
        """Create a new column model for nested struct fields missing from manifest."""
        from dbt2lookml.utils import camel_to_snake
        return DbtModelColumn(
            name=column_name,
            data_type=data_type,
            inner_types=[],
            description=comment,
            meta=DbtModelColumnMeta(),
            lookml_name=camel_to_snake(column_name.split('.')[-1]),
            original_name=original_column_name or column_name,
        )

    def _get_catalog_column_info(
        self, model_id: str, column_name: str
    ) -> Tuple[Optional[str], List[str]]:
        """Get column type information from catalog."""
        if model_id not in self._catalog.nodes:
            return None, []
        catalog_node = self._catalog.nodes[model_id]
        if column_name not in catalog_node.columns:
            return None, []
        column = catalog_node.columns[column_name]
        return column.data_type, column.inner_types or []

    def _update_column_with_inner_types(
        self, column: DbtModelColumn, model_id: str
    ) -> DbtModelColumn:
        """Update a column with type information from catalog."""
        data_type, inner_types = self._get_catalog_column_info(model_id, column.name)
        if data_type:
            column.data_type = data_type
        if inner_types:
            column.inner_types = inner_types
        return column
