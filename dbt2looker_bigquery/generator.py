import lkml
import logging
import json
import os
import re

from dbt2looker_bigquery import models, looker, parser

class LookmlGenerator():

    def __init__(self):
        self.logger = logging.getLogger("rich")

    def _load_file(self, file_path: str) -> dict:
        """Load JSON file from disk.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Dictionary containing the JSON data
        """
        try:
            with open(file_path, 'r') as f:
                raw_file = json.load(f)
        except FileNotFoundError as e:
            self.logger.error(f'Could not find manifest file at {file_path}. Use --target-dir to change the search path for the manifest.json file.')
            raise SystemExit('Failed')

        return raw_file

    def _validate_sql(self, sql: str) -> str:
        """Validate that a string is a valid Looker SQL expression.
        
        Args:
            sql: SQL expression to validate
            
        Returns:
            Validated SQL expression or None if invalid
        """
        sql = sql.strip()
        def check_if_has_dollar_syntax(sql):
            """Check if the string either has ${TABLE}.example or ${view_name}"""
            return '${' in sql and '}' in sql
        
        def check_expression_has_ending_semicolons(sql):
            """Check if the string ends with a semicolon"""
            return sql.endswith(';;')

        if check_expression_has_ending_semicolons(sql):
            self.logger.warning(f"SQL expression {sql} ends with semicolons. It is removed and added by lkml.")
            sql = sql.rstrip(';').rstrip(';').strip()

        if not check_if_has_dollar_syntax(sql):
            self.logger.warning(f"SQL expression {sql} does not contain $TABLE or $view_name")
            return None
        else:
            return sql
    
    def _map_bigquery_to_looker(self, column_type: str) -> str:
        """Map BigQuery data type to Looker data type.
        
        Args:
            column_type: BigQuery data type
            
        Returns:
            Looker data type or None if not supported
        """
        if column_type:
            column_type = column_type.split('<')[0]  # STRUCT< or ARRAY<
            column_type = column_type.split('(')[0]  # Numeric(1,31)

        looker_type = looker.LOOKER_BIGQUERY_DTYPE_MAP.get(column_type)
        if column_type is not None and looker_type is None:
            self.logger.warning(f'Column type {column_type} not supported for conversion from bigquery to looker. No dimension will be created.')

        return looker_type

    def _format_label(self, name: str, remove_date: bool = True) -> str:
        """Format a name into a human-readable label."""
        if remove_date:
            name = name.replace("_date", "")
        return name.replace("_", " ").title()

    def _apply_meta_attributes(self, target_dict: dict, column: models.DbtModelColumn, attributes: list) -> None:
        """Apply meta attributes from column to target dictionary if they exist."""
        if column.meta.looker is not None:
            for attr in attributes:
                value = getattr(column.meta.looker, attr, None)
                if value is not None:
                    if attr == 'value_format_name':
                        target_dict[attr] = value.value
                    else:
                        target_dict[attr] = value

    def _generate_sql(self, column: models.DbtModelColumn, table_format_sql: bool) -> str:
        """Generate SQL expression for a column."""
        if not table_format_sql and '.' in column.name:
            return column.lookml_name
        if '.' in column.name and table_format_sql:
            # For nested fields in the main view, include the parent path
            parent_path = '.'.join(column.name.split('.')[:-1])
            return f'${{{parent_path}}}.{column.lookml_name}'
        return f'${{TABLE}}.{column.name}'

    def _create_iso_field(self, field_type: str, column: models.DbtModelColumn, sql: str) -> dict:
        """Create an ISO year or week field."""
        label_type = field_type.replace("_of_year", "")
        field = {
            'name': f'{column.name}_iso_{field_type}',
            'label': f'{self._format_label(column.name)} ISO {label_type.title()}',
            'type': 'number',
            'sql': f'Extract(iso{label_type} from {sql})',
            'description': f'iso year for {column.name}',
            'group_label': 'D Date',
            'value_format_name': 'id'
        }
        self._apply_meta_attributes(field, column, ['group_label', 'label'])
        if field_type == 'week_of_year':
            field['label'] = field['label'].replace('Week', 'Week Of Year')
        return field
    
    def _get_column_type_category(self, column: models.DbtModelColumn) -> str:
        """Get the category of a column's type."""
        looker_type = self._map_bigquery_to_looker(column.data_type)
        if looker_type in looker.LOOKER_DATE_TIME_TYPES:
            return 'time'
        elif looker_type in looker.LOOKER_DATE_TYPES:
            return 'date'
        return 'scalar'

    def _create_dimension(self, column: models.DbtModelColumn, sql: str, is_hidden: bool = False) -> dict:
        """Create a basic dimension dictionary."""
        data_type = self._map_bigquery_to_looker(column.data_type)
        if data_type is None:
            return None

        dimension = {'name': column.lookml_name}
        
        # Add label if present (should come before type)
        if column.meta.looker is not None and column.meta.looker.label:
            dimension['label'] = column.meta.looker.label
            
        # Add type for scalar types (should come before sql)
        if data_type in looker.LOOKER_SCALAR_TYPES:
            dimension['type'] = data_type
            
        dimension.update({
            'sql': sql,
            'description': column.description or ""
        })
        
        # Add primary key attributes
        if column.is_primary_key:
            dimension['primary_key'] = 'yes'
            dimension['hidden'] = 'yes'
            dimension['value_format_name'] = 'id'
        elif is_hidden:
            dimension['hidden'] = 'yes'
            
        # Apply meta attributes
        self._apply_meta_attributes(dimension, column, ['group_label', 'value_format_name'])
        
        # Handle hidden attribute separately since it needs to be 'yes'/'no'
        if column.meta.looker and column.meta.looker.hidden is not None:
            dimension['hidden'] = 'yes' if column.meta.looker.hidden else 'no'
        
        # Handle array and struct types
        if 'ARRAY' in column.data_type:
            dimension['hidden'] = 'yes'
            dimension['tags'] = ['array']
            dimension.pop('type', None)
        elif 'STRUCT' in column.data_type:
            dimension['hidden'] = 'no'
            dimension['tags'] = ['struct']
            
        return dimension

    def _lookml_dimension_group(
        self, 
        column: models.DbtModelColumn, 
        type: str, 
        table_format_sql: bool, 
        model: models.DbtModel
    ) -> tuple:
        if self._map_bigquery_to_looker(column.data_type) is None:
            raise NotImplementedError()
        else:
            if type == 'time':
                convert_tz = 'yes'
                timeframes = looker.LOOKER_TIME_TIMEFRAMES
                column_name_adjusted = column.name.replace("_date","")
            elif type == 'date':
                convert_tz = 'no'
                timeframes = looker.LOOKER_DATE_TIMEFRAMES
                column_name_adjusted = column.name.replace("_date","")
            else:
                raise NotImplementedError()

            sql = self._generate_sql(column, table_format_sql)

            dimensions = []
            dimension_group = {
                'name': column_name_adjusted,
                'label': self._format_label(column.lookml_name),
                'type': type,
                'sql': sql,
                'description': column.description,
                'datatype': self._map_bigquery_to_looker(column.data_type),
                'timeframes': timeframes,
                'group_label': 'D Date' if column_name_adjusted == 'd' else f'{self._format_label(column.lookml_name)}',
                'convert_tz': convert_tz
            }
            self._apply_meta_attributes(dimension_group, column, ['group_label', 'label'])

            dimension_group_set = {
                'name' : f's_{column_name_adjusted}',
                'fields': [
                    f"{column_name_adjusted}_{looker_time_timeframe}" for looker_time_timeframe in timeframes
                ]
            }

            if type == 'date':
                iso_year = self._create_iso_field('year', column, sql)
                iso_week_of_year = self._create_iso_field('week_of_year', column, sql)
                dimensions = [iso_year, iso_week_of_year]
                dimension_group_set['fields'].extend([f"{column.name}_iso_year", f"{column.name}_iso_week_of_year"])

            return dimension_group, dimension_group_set, dimensions


    def _lookml_dimensions_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> tuple:
        dimensions = []
        table_format_sql = True
        is_hidden = False
        nested_dimensions = []

        # First add ISO date dimensions for main view only
        if not include_names:  # Only for main view
            for column in model.columns.values():
                if column.data_type == 'DATE':
                    _, _, dimension_group_dimensions = self._lookml_dimension_group(column, 'date', table_format_sql, model)
                    if dimension_group_dimensions:
                        dimensions.extend(dimension_group_dimensions)

        # Then add regular dimensions
        for column in model.columns.values():
            if include_names:
                table_format_sql = False

                # For nested views, only include fields that are children of the parent
                # or the parent itself if it's a simple array
                parent = include_names[0] if include_names else None
                if column.name == parent:
                    # Keep parent field only if it's a simple array (e.g. ARRAY<INT64>)
                    if not (column.data_type == 'ARRAY' and len(column.inner_types) == 1 and ' ' not in column.inner_types[0]):
                        continue
                    # For simple arrays in nested views, use the inner type
                    if column.data_type == 'ARRAY' and len(column.inner_types) == 1 and ' ' not in column.inner_types[0]:
                        data_type = self._map_bigquery_to_looker(column.inner_types[0])
                        field_name = column.lookml_name  # Use the array field name
                        sql = field_name  # In nested view, use field name directly
                        dimension = {
                            'name': field_name,
                            'type': data_type,
                            'sql': sql,
                            'description': column.description or ""
                        }
                        dimensions.append(dimension)
                        continue
                elif not column.name.startswith(f"{parent}."):
                    continue

            if column.name in exclude_names or column.name == 'md_insert_dttm':
                continue

            # Skip date dimensions since we handled them above
            if column.data_type == 'DATE' and not include_names:
                continue

            data_type = self._map_bigquery_to_looker(column.data_type)
            
            if data_type is None:
                self.logger.debug(f"{model.name} {column.name} is not part of catalog table. Skipping.")
                continue
            
            sql = self._generate_sql(column, table_format_sql)
            dimension = self._create_dimension(column, sql)
            if dimension is not None:
                dimensions.append(dimension)

        return dimensions, nested_dimensions

    def _lookml_measures_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> list:
        # Initialize an empty list to hold all lookml measures.
        lookml_measures = []
        table_format_sql = True

        # Iterate over all columns in the model.
        for column in model.columns.values():
            
            if include_names:
                table_format_sql = False

                # For nested fields, if any parent is in include_names, include this field
                if column.name not in include_names and not any(parent in include_names for parent in column.name.split('.')):
                    continue

            if len(exclude_names) > 0:
                if column.name in exclude_names:
                    continue

            if self._map_bigquery_to_looker(column.data_type) in looker.LOOKER_SCALAR_TYPES:

                if hasattr(column.meta, 'looker_measures'):
                    # For each measure found in the combined dictionary, create a lookml_measure.
                    for measure in column.meta.looker_measures:
                        # Call the lookml_measure function and append the result to the list.
                        lookml_measures.append(self._lookml_measure(column, measure, table_format_sql, model))

        # Return the list of lookml measures.
        return lookml_measures

    def _apply_measure_attributes(self, measure_dict: dict, measure: models.DbtMetaMeasure) -> None:
        """Apply measure attributes to the measure dictionary."""
        # Simple attributes that can be directly copied
        direct_attributes = [
            'approximate', 'approximate_threshold', 'allow_approximate_optimization',
            'can_filter', 'tags', 'alias', 'convert_tz', 'suggestable', 'precision',
            'percentile', 'group_label', 'label', 'description'
        ]
        
        for attr in direct_attributes:
            value = getattr(measure, attr, None)
            if value is not None:
                measure_dict[attr] = value

        # Special handling for hidden attribute which has a value property
        if measure.hidden is not None:
            measure_dict['hidden'] = measure.hidden.value

    def _lookml_measure(
        self, 
        column: models.DbtModelColumn, 
        measure: models.DbtMetaMeasure, 
        table_format_sql: bool, 
        model: models.DbtModel
    ) -> dict:
        """Create a LookML measure from a DBT model column and measure."""
        if measure.type.value not in looker.LOOKER_BIGQUERY_MEASURE_TYPES:
            self.logger.warning(f"Measure type {measure.type.value} not supported for conversion to looker. No measure will be created.")
            return None
        
        m = {
            'name': f'm_{measure.type.value}_{column.name}',
            'type': measure.type.value,
            'sql': self._generate_sql(column, table_format_sql),
            'description': f'{measure.type.value} of {column.name}',
        }

        # Handle value format inheritance
        if measure.value_format_name is not None:
            m['value_format_name'] = measure.value_format_name.value
        elif column.meta.looker is not None and column.meta.looker.value_format_name is not None:
            m['value_format_name'] = column.meta.looker.value_format_name.value

        # Handle SQL expressions
        if measure.sql is not None:
            validated_sql = self._validate_sql(measure.sql)
            if validated_sql is not None:
                m['sql'] = validated_sql
                if measure.type.value != 'number':
                    self.logger.warn(f"SQL expression {measure.sql} is not a number type measure. It is overwritten to be number since SQL is set.")
                    m['type'] = 'number'

        if measure.sql_distinct_key is not None:
            validated_sql = self._validate_sql(measure.sql_distinct_key)
            if validated_sql is not None:
                m['sql_distinct_key'] = validated_sql
            else:
                self.logger.warn(f"SQL expression {measure.sql_distinct_key} is not valid. It is not set as sql_distinct_key.")

        # Apply all other measure attributes
        self._apply_measure_attributes(m, measure)

        self.logger.debug(f"measure created: {m}")
        return m

    def _extract_array_models(self, columns: list[models.DbtModelColumn]) -> list[models.DbtModelColumn]:
        """Extract array models from a list of columns.
        
        Args:
            columns: List of columns
            
        Returns:
            List of array models
        """
        array_list = []

        # Initialize parent_list with all columns that are arrays
        for column in columns:
            if column.data_type is not None:
                if 'ARRAY' == column.data_type:
                    array_list.append(column)
                    
        return array_list


    def _group_strings(self, all_columns: list[models.DbtModelColumn], array_columns: list[models.DbtModelColumn]) -> dict:
        """Group strings into a nested structure.
        
        Args:
            all_columns: List of all columns
            array_columns: List of array columns
            
        Returns:
            Dictionary containing the nested structure
        """
        nested_columns = {}

        def remove_parts(input_string):
            parts = input_string.split('.')
            modified_parts = parts[:-1]
            result = '.'.join(modified_parts)
            return result
        
        def is_single_type_array(column: models.DbtModelColumn):
            if column.data_type == 'ARRAY':
                if len(column.inner_types) == 1 and ' ' not in column.inner_types[0]: #TODO: Improve to make sure this is a single type
                    return True
            return False
        
        def recurse(parent: models.DbtModelColumn, all_columns: list[models.DbtModelColumn], level = 0):
            structure = {
                'column' : parent,
                'children' : []
            }

            self.logger.debug(f"level {level}, {parent.name}")
            for column in all_columns:
                    
                if column.data_type in ('ARRAY', 'STRUCT'):
                    # If ARRAY<INT64> or likeworthy
                    if is_single_type_array(column):
                        structure['children'].append({column.name : {'column' : column, 'children' : []}}) 
                    # Normal ARRAY or STRUCT
                    else:
                        structure['children'].append({column.name : recurse(
                                    parent = column,
                                    all_columns = [d for d in all_columns if remove_parts(d.name) == column.name],
                                    level=level+1)})  
                else:
                    structure['children'].append({column.name : {'column' : column, 'children' : []}})    

            return structure
        
        for parent in array_columns:
            nested_columns[parent.name] = recurse(parent, [d for d in all_columns if remove_parts(d.name) == parent.name])
            self.logger.debug('nested_parent: %s', parent.name)

        return nested_columns

    def recurse_joins(self, structure, model, use_table_name_as_view=False):
        join_list = []
        for parent, children in structure.items():
            # Use table name from relation_name if use_table_name_as_view is True
            base_name = model.relation_name.split('.')[-1].strip('`') if use_table_name_as_view else model.name 
            view_name = f"{base_name}__{parent.replace('.','__')}"
            
            # Create SQL join for array unnesting
            join_sql = f'LEFT JOIN UNNEST(${{{base_name}.{parent}}}) AS {view_name}'
            
            join_list.append({
                'name': view_name,
                'relationship': 'one_to_many',
                'sql': join_sql,
                'type': 'left_outer',
                'required_joins': [],  # No required joins for top-level arrays
            })

            # Process nested arrays within this array
            for child_structure in children['children']:
                for child_name, child_dict in child_structure.items():
                    if len(child_dict['children']) > 0:
                        child_view_name = f"{base_name}__{child_name.replace('.','__')}"
                        child_join_sql = f'LEFT JOIN UNNEST(${{{view_name}.{child_name.split(".")[-1]}}}) AS {child_view_name}'
                        
                        join_list.append({
                            'name': child_view_name,
                            'relationship': 'one_to_many',
                            'sql': child_join_sql,
                            'type': 'left_outer',
                            'required_joins': [view_name],  # This join requires the parent view
                        })
                        
                        # Recursively process any deeper nested arrays
                        join_list.extend(self.recurse_joins(child_structure, model, use_table_name_as_view))

        return join_list

    def _get_view_label(self, model: models.DbtModel) -> str:
        """Get the view label from the model metadata or name.
        
        Args:
            model: DBT model
            
        Returns:
            View label string
        """
        # Check looker meta label first
        if hasattr(model.meta.looker, 'label') and model.meta.looker.label is not None:
            return model.meta.looker.label
            
        # Fall back to model name if available
        if hasattr(model, 'name'):
            return model.name.replace("_", " ").title()
            
        self.logger.warning(f"Model has no name")
        return None

    def _get_excluded_array_names(self, model: models.DbtModel, array_models: list) -> list:
        """Get list of dimension names to exclude from main view (array fields and their children).
        
        Args:
            model: DBT model
            array_models: List of array type columns
            
        Returns:
            List of dimension names to exclude
        """
        exclude_names = []
        for array_model in array_models:
            # Don't exclude the array field itself from main view
            # exclude_names.append(array_model.name)
            for col in model.columns.values():
                if col.name.startswith(f"{array_model.name}."):
                    exclude_names.append(col.name)
        return exclude_names

    def _create_main_view(
        self, 
        model: models.DbtModel, 
        view_name: str, 
        view_label: str, 
        exclude_names: list
    ) -> dict:
        """Create the main view definition.
        
        Args:
            model: DBT model
            view_name: Name for the view
            view_label: Label for the view
            exclude_names: Names of dimensions to exclude
            
        Returns:
            Dictionary containing the main view definition
        """
        # Build view dict in specific order to match expected LookML output
        view = {
            'name': view_name,
            'label': view_label,
            'sql_table_name': model.relation_name,
        }

        # Add dimensions
        dimensions, nested_dimensions = self._lookml_dimensions_from_model(model, exclude_names=exclude_names)
        if dimensions:
            view['dimensions'] = dimensions

        # Add dimension_groups
        dimension_groups = self._lookml_dimension_groups_from_model(model, exclude_names=exclude_names).get('dimension_groups')
        if dimension_groups:
            view['dimension_groups'] = dimension_groups

        # Then add measures
        measures = self._lookml_measures_from_model(model, exclude_names=exclude_names)
        if measures:
            view['measures'] = measures

        # Finally add sets
        sets = self._lookml_dimension_groups_from_model(model, exclude_names=exclude_names).get('dimension_group_sets')
        if sets:
            view['sets'] = sets

        return view

    def _create_nested_view(
        self, 
        model: models.DbtModel, 
        base_name: str, 
        array_model: models.DbtModelColumn, 
        view_label: str,
        use_table_name_as_view: bool = False
    ) -> dict:
        """Create a nested view definition for an array field.
        
        Args:
            model: DBT model
            base_name: Base name for the view
            array_model: The array column to create view for
            view_label: Label for the view
            use_table_name_as_view: Whether to use table name for view name
            
        Returns:
            Dictionary containing the nested view definition
        """
        # Use table name if flag is set
        if use_table_name_as_view:
            nested_view_name = f"{model.relation_name.split('.')[-1].strip('`')}__{array_model.name.replace('.', '__')}"
        else:
            nested_view_name = f"{base_name}__{array_model.name.replace('.', '__')}"
            
        include_names = [array_model.name]
        for col in model.columns.values():
            if col.name.startswith(f"{array_model.name}."):
                include_names.append(col.name)

        dimensions, nested_dimensions = self._lookml_dimensions_from_model(model, include_names=include_names)
        nested_view = {
            'name': nested_view_name,
            'label': view_label
        }
        if dimensions:
            nested_view['dimensions'] = dimensions

        # Add dimension_groups
        dimension_groups = self._lookml_dimension_groups_from_model(model, include_names=include_names).get('dimension_groups')
        if dimension_groups:
            nested_view['dimension_groups'] = dimension_groups

        # Then add measures
        measures = self._lookml_measures_from_model(model, include_names=include_names)
        if measures:
            nested_view['measures'] = measures

        # Finally add sets
        sets = self._lookml_dimension_groups_from_model(model, include_names=include_names).get('dimension_group_sets')
        if sets:
            nested_view['sets'] = sets

        return nested_view

    def _create_explore(
        self, 
        model: models.DbtModel, 
        view_name: str, 
        view_label: str, 
        structure: dict, 
        use_table_name_as_view: bool
    ) -> dict:
        """Create the explore definition.
        
        Args:
            model: DBT model
            view_name: Name of the main view
            view_label: Label for the explore
            structure: Nested structure information
            use_table_name_as_view: Whether to use table name as view name
            
        Returns:
            Dictionary containing the explore definition
        """
        # Check if model.meta.looker exists and has hidden attribute
        hidden = 'no'
        if hasattr(model, 'meta') and hasattr(model.meta, 'looker') and hasattr(model.meta.looker, 'hidden'):
            hidden = 'yes' if model.meta.looker.hidden else 'no'
        
        return {
            'name': view_name,
            'label': view_label,
            'from': view_name,
            'joins': self.recurse_joins(structure, model, use_table_name_as_view),
            'hidden': hidden
        }

    def _lookml_dimension_groups_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> dict:
        """Get dimension groups from a model.
        
        Args:
            model: DBT model
            include_names: Optional list of names to include
            exclude_names: Optional list of names to exclude
            
        Returns:
            Dictionary containing dimension groups and sets
        """
        dimension_groups = []
        dimension_group_sets = []
        table_format_sql = True

        if include_names:
            table_format_sql = False

        for column in model.columns.values():
            if include_names:
                if column.name not in include_names:
                    continue

            if len(exclude_names) > 0:
                if column.name in exclude_names:
                    continue

            column_type = self._get_column_type_category(column)
            if column_type in ('time', 'date'):
                dimension_group, dimension_group_set, dimensions = self._lookml_dimension_group(
                    column=column,
                    type=column_type,
                    table_format_sql=table_format_sql,
                    model=model
                )
                dimension_groups.append(dimension_group)
                dimension_group_sets.append(dimension_group_set)

        return {
            'dimension_groups': dimension_groups if dimension_groups else None,
            'dimension_group_sets': dimension_group_sets if dimension_group_sets else None
        }

    def _write_lookml_file(
        self, 
        output_dir: str, 
        model: models.DbtModel, 
        view_name: str, 
        contents: str,
        use_table_name_as_view: bool = False
    ) -> str:
        """Write LookML content to a file.
        
        Args:
            output_dir: Directory to write to
            model: DBT model
            view_name: Name of the view
            contents: LookML content to write
            use_table_name_as_view: Whether to use table name for file naming
            
        Returns:
            Path to the written file
        """
        # Create directory structure
        path = os.path.join(output_dir, model.path.split(model.name)[0])
        os.makedirs(path, exist_ok=True)
        
        # Determine file name based on flag
        if use_table_name_as_view:
            file_name = model.relation_name.split('.')[-1].strip('`')
        else:
            file_name = view_name
            
        file_path = f'{path}/{file_name}.view.lkml'
        
        # Write contents
        with open(file_path, 'w') as f:
            f.truncate()
            f.write(contents)
            
        self.logger.debug(f'Generated {file_path}')
        return file_path

    def _get_model_key(self, model: models.DbtModel, use_table_name_as_view: bool) -> str:
        """Get the key to use for the model in locale file.
        
        Args:
            model: DBT model
            use_table_name_as_view: Whether to use table name instead of model name
            
        Returns:
            String key for the model
        """
        if use_table_name_as_view:
            # Extract table name from relation_name (e.g., `project.dataset.table_name` -> table_name)
            return model.relation_name.split('.')[-1].strip('`')
        return model.name

    def lookml_view_from_dbt_model(
        self, 
        model: models.DbtModel, 
        output_dir: str, 
        skip_explore_joins: bool, 
        use_table_name_as_view: bool = False,
        locale_mapping: dict = None,
        generate_locale: bool = False
    ):
        """Create LookML views and explore from a DBT model.
        
        Args:
            model: DBT model to convert
            output_dir: Directory to write output files
            skip_explore_joins: Whether to skip creating explore joins
            use_table_name_as_view: Whether to use table name as view name
            locale_mapping: Dictionary to map labels to identifiers
            generate_locale: Whether to generate locale files
            
        Returns:
            Dictionary containing the generated LookML
        """
        self.logger.info(f"Starting processing of {model.name}")
        
        # Extract array fields and structure
        array_models = self._extract_array_models(model.columns.values())
        structure = self._group_strings(model.columns.values(), array_models)
        
        # Get view label and excluded names
        view_label = self._get_view_label(model)
        if locale_mapping and view_label in locale_mapping:
            view_label = locale_mapping[view_label]
        exclude_names = self._get_excluded_array_names(model, array_models)

        # Get base name and view name
        base_name = model.relation_name.split('.')[-1].strip('`') if use_table_name_as_view else model.name
        view_name = base_name

        # Create main view first
        main_view = self._create_main_view(model, view_name, view_label, exclude_names)
        
        # Update dimension labels to use locale references only if locale generation is enabled
        if generate_locale:
            model_key = self._get_model_key(model, use_table_name_as_view)
            if 'dimensions' in main_view:
                for dim in main_view['dimensions']:
                    if 'label' in dim:
                        dim['label'] = f"models.{model_key}.{dim['name']}"

        lookml_list = [main_view]

        # Create nested views for arrays
        for array_model in array_models:
            nested_view = self._create_nested_view(
                model=model,
                base_name=base_name,
                array_model=array_model,
                view_label=view_label,
                use_table_name_as_view=use_table_name_as_view
            )
            lookml_list.append(nested_view)

        # Create explore if needed
        lookml = {'view': lookml_list}  # Keep main view first, followed by nested views
        if len(array_models) > 0 and not skip_explore_joins:
            explore = self._create_explore(model, view_name, view_label, structure, use_table_name_as_view)
            lookml['explore'] = explore

        # Write LookML file
        try:
            return self._write_lookml_file(
                output_dir=output_dir,
                model=model,
                view_name=view_name,
                contents=lkml.dump(lookml),
                use_table_name_as_view=use_table_name_as_view
            )
        except Exception as e:
            self.logger.error(f"Failed to write LookML file for {model.name}: {str(e)}")
            raise

    def generate(
        self, 
        target_dir: str, 
        tag: str, 
        output_dir: str, 
        skip_explore_joins: bool = False, 
        exposures_only: bool = False, 
        exposures_tag: str = None, 
        select: str = None, 
        use_table_name_as_view: bool = False,
        generate_locale: bool = False
    ):
        """Generate LookML views and explores from DBT models.
        
        Args:
            target_dir: Directory containing DBT files
            tag: Tag to filter DBT models
            output_dir: Directory to write output files
            skip_explore_joins: Whether to skip creating explore joins
            exposures_only: Whether to only generate exposures
            exposures_tag: Tag to filter exposures
            select: Model to select
            use_table_name_as_view: Whether to use table name as view name
            generate_locale: Whether to generate locale files
            
        Returns:
            List of generated LookML files
        """
        # Load dbt files
        raw_manifest = self._load_file(os.path.join(target_dir, 'manifest.json'))
        raw_catalog = self._load_file(os.path.join(target_dir, 'catalog.json'))
        
        # Get dbt models
        dbt_parser = parser.DbtParser(raw_manifest, raw_catalog)
        dbt_models = dbt_parser.parse_typed_models(tag=tag, exposures_only=exposures_only, exposures_tag=exposures_tag, select_model=select)

        # Generate lookml views
        lookml_views = [
            self.lookml_view_from_dbt_model(model, output_dir, skip_explore_joins, use_table_name_as_view, generate_locale=generate_locale)
            for model in dbt_models
        ]
        
        # Generate locale file if requested
        if generate_locale:
            locale_data = {"models": {}}
            for model in dbt_models:
                model_key = self._get_model_key(model, use_table_name_as_view)
                model_labels = {}
                for field in model.columns.values():
                    model_labels[field.name] = field.lookml_name.replace("_", " ").title()
                locale_data["models"][model_key] = model_labels
            
            locale_file_path = os.path.join(output_dir, "en.strings.json")
            with open(locale_file_path, 'w') as locale_file:
                json.dump(locale_data, locale_file, indent=4)
        
        return lookml_views


class NotImplementedError(Exception):
    pass
