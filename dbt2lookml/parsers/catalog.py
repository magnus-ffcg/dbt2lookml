"""Catalog-specific parsing functionality."""

from typing import List, Optional, Tuple

from dbt2lookml.models.dbt import DbtCatalog, DbtModel, DbtModelColumn, DbtModelColumnMeta


class CatalogParser:
    """Parser for DBT catalog information."""

    def __init__(self, catalog: DbtCatalog):
        """Initialize with catalog data."""
        self._catalog = catalog

    def process_model_columns(self, model: DbtModel) -> Optional[DbtModel]:
        """Process a model by updating its columns with catalog information."""
        processed_columns = {
            column_name: (
                processed_column
                if (
                    processed_column := self._update_column_with_inner_types(
                        column, model.unique_id
                    )
                )
                else column
            )
            for column_name, column in model.columns.items()
        }
        # Create missing array columns
        if model.unique_id in self._catalog.nodes:
            catalog_node = self._catalog.nodes[model.unique_id]
            for column_name, column in catalog_node.columns.items():
                if (
                    column_name not in processed_columns
                    and 'ARRAY' in f'{column.data_type}'
                    and column.data_type is not None
                ):
                    processed_columns[column_name] = self._create_missing_array_column(
                        column_name, column.data_type, column.inner_types or []
                    )

        # Always return the model, even if no columns were processed
        return (
            model.model_copy(update={'columns': processed_columns}) if processed_columns else model
        )

    def _create_missing_array_column(
        self, column_name: str, data_type: str, inner_types: List[str]
    ) -> DbtModelColumn:
        """Create a new column model for array columns missing from manifest."""
        return DbtModelColumn(
            name=column_name,
            data_type=data_type,
            inner_types=inner_types,
            description=None,
            meta=DbtModelColumnMeta(),
            lookml_name=column_name,
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
