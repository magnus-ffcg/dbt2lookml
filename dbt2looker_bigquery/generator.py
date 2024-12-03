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

    def _lookml_dimension_group(
        self, 
        column: models.DbtModelColumn, 
        type: str, 
        table_format_sql: bool, 
        model: models.DbtModel
    ) -> tuple:
        """Create a LookML dimension group from a DBT model column.
        
        Args:
            column: DBT model column
            type: Type of dimension group (time or date)
            table_format_sql: Whether to use table format SQL
            model: DBT model
            
        Returns:
            Tuple containing the dimension group, dimension group set, and dimensions
        """
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

            # For nested views, use just the field name without ${TABLE}
            if not table_format_sql and '.' in column.name:
                field_name = column.lookml_name
                sql = field_name
            else:
                sql = f'${{TABLE}}.{column.name}'

            dimensions = []
            dimension_group = {
                'name': column_name_adjusted,
                'label': column.lookml_name.replace("_date","").replace("_", " ").title(),
                'type': type,
                'sql': sql,
                'description': column.description,
                'datatype': self._map_bigquery_to_looker(column.data_type),
                'timeframes': timeframes,
                'group_label': f'{column.lookml_name.replace("_", " ").title()}',
                'convert_tz': convert_tz
            }
            if column.meta.looker.label != None:
                dimension_group['label'] = column.meta.looker.label
            if column.meta.looker.group_label != None:
                dimension_group['group_label'] = column.meta.looker.group_label

            dimension_group_set = {
                'name' : f's_{column_name_adjusted}',
                'fields': [
                    f"{column_name_adjusted}_{looker_time_timeframe}" for looker_time_timeframe in timeframes
                ]
            }

            if type == 'date':
                iso_year = {
                    'name': f'{column.name}_iso_year',
                    'label': f'{column.name.replace("_date","").replace("_"," ").title()} ISO Year',
                    'type': 'number',
                    'sql': f'Extract(isoyear from {sql})',
                    'description': f'iso year for {column.name}',
                    'group_label': f'{column.lookml_name.replace("_", " ").title()}',
                    'value_format_name': 'id'
                }
                if column.meta.looker.group_label != None:
                    iso_year['group_label'] = column.meta.looker.group_label
                if column.meta.looker.label != None:
                    iso_year['label'] = f"{column.meta.looker.label} ISO Year"

                iso_week_of_year = {
                    'name': f'{column.name}_iso_week_of_year',
                    'label': f'{column.name.replace("_date","").replace("_"," ").title()} ISO Week Of Year',
                    'type': 'number',
                    'sql': f'Extract(isoweek from {sql})',
                    'description': f'iso year for {column.name}',
                    'group_label': f'{column.lookml_name.replace("_", " ").title()}',
                    'value_format_name': 'id'
                }
                if column.meta.looker.group_label != None:
                    iso_week_of_year['group_label'] = column.meta.looker.group_label
                if column.meta.looker.label != None:
                    iso_week_of_year['label'] = f"{column.meta.looker.label} ISO Week Of Year"

                dimensions = [iso_year, iso_week_of_year]
                dimension_group_set['fields'].extend([f"{column.name}_iso_year", f"{column.name}_iso_week_of_year"])

            return dimension_group, dimension_group_set, dimensions


    def _lookml_dimensions_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> tuple:
        """Create LookML dimensions from a DBT model.
        
        Args:
            model: DBT model
            include_names: List of names to include
            exclude_names: List of names to exclude
            
        Returns:
            Tuple containing the dimensions and nested dimensions
        """
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

            if column.name in exclude_names:
                continue

            # Skip date dimensions since we handled them above
            if column.data_type == 'DATE' and not include_names:
                continue

            data_type = self._map_bigquery_to_looker(column.data_type)
            
            if data_type is None:
                self.logger.debug(f"{model.name} {column.name} is not part of catalog table. Skipping.")
                continue
            
            if data_type in looker.LOOKER_SCALAR_TYPES or data_type in ('ARRAY', 'STRUCT'):
                # For nested views, use just the field name without ${TABLE}
                if include_names:
                    # Get just the last part of the field name after the parent
                    field_name = column.lookml_name  # Use lookml_name which is the last part
                    sql = field_name
                else:
                    field_name = column.lookml_name
                    if '.' in column.name:
                        # For nested fields in the main view, include the parent path
                        parent_path = '.'.join(column.name.split('.')[:-1])
                        sql = f'${{{parent_path}}}.{field_name}'
                    else:
                        sql = f'${{TABLE}}.{column.name}'

                # Build dimension dict in specific order to match expected LookML output
                dimension = {'name': field_name}  # Always first

                # Add label if present (should come before type)
                if column.meta.looker is not None and column.meta.looker.label:
                    dimension['label'] = column.meta.looker.label

                # Add type for scalar types (should come before sql)
                if data_type in looker.LOOKER_SCALAR_TYPES:
                    dimension['type'] = data_type

                # Add sql and description
                dimension['sql'] = sql
                dimension['description'] = column.description or ""

                # Add remaining metadata in order
                if column.is_primary_key:
                    dimension['primary_key'] = 'yes'
                    is_hidden = True
                    dimension['value_format_name'] = 'id'

                if column.meta.looker is not None:
                    if column.meta.looker.group_label:
                        dimension['group_label'] = column.meta.looker.group_label
                    if column.meta.looker.hidden is not None:
                        dimension['hidden'] = 'yes' if column.meta.looker.hidden else 'no'
                    elif is_hidden:
                        dimension['hidden'] = 'yes'
                    if column.meta.looker.value_format_name:
                        dimension['value_format_name'] = column.meta.looker.value_format_name.value

                # Handle array types
                if 'ARRAY' in column.data_type:
                    dimension['hidden'] = 'yes'
                    dimension['tags'] = ['array']
                    dimension.pop('type', None)
                # Handle struct types
                elif 'STRUCT' in column.data_type:
                    dimension['hidden'] = 'no'
                    dimension['tags'] = ['struct']

                is_hidden = False
                dimensions.append(dimension)

        return dimensions, nested_dimensions

    def _lookml_dimension_groups_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> dict:
        """Create LookML dimension groups from a DBT model.
        
        Args:
            model: DBT model
            include_names: List of names to include
            exclude_names: List of names to exclude
            
        Returns:
            Dictionary containing the dimension groups and dimension group sets
        """
        dimension_groups = []
        dimension_group_sets = []
        table_format_sql = True

        for column in model.columns.values(): 

            if include_names:
                table_format_sql = False

                # For nested fields, if any parent is in include_names, include this field
                if column.name not in include_names and not any(parent in include_names for parent in column.name.split('.')):
                    continue

            if len(exclude_names) > 0:
                if column.name in exclude_names:
                    continue

            if self._map_bigquery_to_looker(column.data_type) in looker.LOOKER_DATE_TIME_TYPES: 
                dimension_group, dimension_set, _ = self._lookml_dimension_group(column, 'time', table_format_sql, model)            
            elif self._map_bigquery_to_looker(column.data_type) in looker.LOOKER_DATE_TYPES:
                dimension_group, dimension_set, _ = self._lookml_dimension_group(column, 'date', table_format_sql, model)            
            else:
                continue    

            dimension_groups.append(dimension_group)
            dimension_group_sets.append(dimension_set)

        return {'dimension_groups' : dimension_groups, 'dimension_group_sets': dimension_group_sets}


    def _lookml_measures_from_model(
        self, 
        model: models.DbtModel, 
        include_names: list = None, 
        exclude_names: list = []
    ) -> list:
        """Create LookML measures from a DBT model.
        
        Args:
            model: DBT model
            include_names: List of names to include
            exclude_names: List of names to exclude
            
        Returns:
            List of LookML measures
        """
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

    def _lookml_measure(
        self, 
        column: models.DbtModelColumn, 
        measure: models.DbtMetaMeasure, 
        table_format_sql: bool, 
        model: models.DbtModel
    ) -> dict:
        """Create a LookML measure from a DBT model column and measure.
        
        Args:
            column: DBT model column
            measure: DBT meta measure
            table_format_sql: Whether to use table format SQL
            model: DBT model
            
        Returns:
            Dictionary containing the LookML measure
        """
        if measure.type.value not in looker.LOOKER_BIGQUERY_MEASURE_TYPES:
            self.logger.warning(f"Measure type {measure.type.value} not supported for conversion to looker. No measure will be created.")
            return None
        
        m = {
            'name': f'm_{measure.type.value}_{column.name}',
            'type': measure.type.value,
            'sql':  f'${{{column.name}}}' if table_format_sql else f'${{{model.name}__{column.name}}}',
            'description': f'{measure.type.value} of {column.name}',
        }

        # inherit the value format, or overwrite it
        if measure.value_format_name != None:
            m['value_format_name'] = measure.value_format_name.value
        elif column.meta.looker != None:
            if column.meta.looker.value_format_name != None:
                m['value_format_name'] = column.meta.looker.value_format_name.value        

        # allow configuring advanced lookml measures
        if measure.sql != None:
            validated_sql = self._validate_sql(measure.sql)
            if validated_sql is not None:
                m['sql'] = validated_sql
                if measure.type.value != 'number':
                    self.logger.warn(f"SQL expression {measure.sql} is not a number type measure. It is overwritten to be number since SQL is set.")
                    m['type'] = 'number'
        
        if measure.sql_distinct_key != None:
            validated_sql = self._validate_sql(measure.sql_distinct_key)
            if validated_sql is not None:
                m['sql_distinct_key'] = validated_sql
            else:
                self.logger.warn(f"SQL expression {measure.sql_distinct_key} is not valid. It is not set as sql_distinct_key.")

        if measure.approximate != None:
            m['approximate'] = measure.approximate
        
        if measure.approximate_threshold != None:
            m['approximate_threshold'] = measure.approximate_threshold
        
        if measure.allow_approximate_optimization != None:
            m['allow_approximate_optimization'] = measure.allow_approximate_optimization
        
        if measure.can_filter != None:
            m['can_filter'] = measure.can_filter

        if measure.tags != None:
            m['tags'] = measure.tags

        if measure.alias != None:
            m['alias'] = measure.alias
        
        if measure.convert_tz != None:
            m['convert_tz'] = measure.convert_tz

        if measure.suggestable != None:
            m['suggestable'] = measure.suggestable

        if measure.precision != None:
            m['precision'] = measure.precision

        if measure.percentile != None:
            m['percentile'] = measure.percentile

        if measure.group_label != None:
            m['group_label'] = measure.group_label

        if measure.label != None:
            m['label'] = measure.label

        if measure.hidden != None:
            m['hidden'] = measure.hidden.value

        if measure.description != None:
            m['description'] = measure.description

        self.logger.debug(f"measure created: {m}" )
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
        hidden = 'yes' if not hasattr(model.meta.looker, 'hidden') else ('yes' if model.meta.looker.hidden else 'no')
        
        return {
            'name': view_name,
            'label': view_label,
            'from': view_name,
            'joins': self.recurse_joins(structure, model, use_table_name_as_view),
            'hidden': hidden
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

    def lookml_view_from_dbt_model(
        self, 
        model: models.DbtModel, 
        output_dir: str, 
        skip_explore_joins: bool, 
        use_table_name_as_view: bool = False
    ):
        """Create LookML views and explore from a DBT model.
        
        Args:
            model: DBT model to convert
            output_dir: Directory to write output files
            skip_explore_joins: Whether to skip creating explore joins
            use_table_name_as_view: Whether to use table name as view name
            
        Returns:
            Dictionary containing the generated LookML
        """
        self.logger.info(f"Starting processing of {model.name}")
        
        # Extract array fields and structure
        array_models = self._extract_array_models(model.columns.values())
        structure = self._group_strings(model.columns.values(), array_models)
        
        # Get view label and excluded names
        view_label = self._get_view_label(model)
        exclude_names = self._get_excluded_array_names(model, array_models)

        # Get base name and view name
        base_name = model.relation_name.split('.')[-1].strip('`') if use_table_name_as_view else model.name
        view_name = base_name

        # Create main view first
        main_view = self._create_main_view(model, view_name, view_label, exclude_names)
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
        use_table_name_as_view: bool = False
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
            self.lookml_view_from_dbt_model(model, output_dir, skip_explore_joins, use_table_name_as_view)
            for model in dbt_models
        ]
        
        return lookml_views


class NotImplementedError(Exception):
    pass
