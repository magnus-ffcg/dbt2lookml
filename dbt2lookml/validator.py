"""LookML Validator for dbt2lookml generated files.

This module validates that generated .view.lkml files contain valid LookML syntax
and structure according to Looker's specifications.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
import lkml


class LookMLValidationError(Exception):
    """Exception raised when LookML validation fails."""
    
    def __init__(self, message: str, line_number: Optional[int] = None, file_path: Optional[str] = None):
        self.message = message
        self.line_number = line_number
        self.file_path = file_path
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format the error message with context."""
        parts = []
        if self.file_path:
            parts.append(f"File: {self.file_path}")
        if self.line_number:
            parts.append(f"Line: {self.line_number}")
        parts.append(f"Error: {self.message}")
        return " | ".join(parts)


class LookMLValidator:
    """Validates LookML syntax and structure for dbt2lookml generated files."""
    
    # Valid LookML field types
    VALID_DIMENSION_TYPES = {
        'string', 'number', 'yesno', 'date', 'date_time', 'time', 'duration',
        'location', 'zipcode', 'tier'
    }
    
    VALID_MEASURE_TYPES = {
        'count', 'count_distinct', 'sum', 'average', 'min', 'max', 'median',
        'percentile', 'list', 'number'
    }
    
    VALID_DIMENSION_GROUP_TYPES = {'time', 'duration'}
    
    VALID_TIMEFRAMES = {
        'raw', 'time', 'date', 'week', 'month', 'quarter', 'year',
        'week_of_year', 'month_of_year', 'quarter_of_year', 'day_of_week',
        'day_of_month', 'day_of_year', 'hour', 'minute', 'second',
        'hour_of_day', 'minute_of_hour', 'second_of_minute'
    }
    
    VALID_DATATYPES = {'date', 'datetime', 'timestamp', 'epoch'}
    
    VALID_RELATIONSHIP_TYPES = {'one_to_one', 'one_to_many', 'many_to_one', 'many_to_many'}
    
    def __init__(self):
        self.errors: List[LookMLValidationError] = []
        self.warnings: List[str] = []
    
    def validate_file(self, file_path: Union[str, Path]) -> Tuple[bool, List[LookMLValidationError], List[str]]:
        """Validate a single LookML file.
        
        Args:
            file_path: Path to the .view.lkml file to validate
            
        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        self.errors = []
        self.warnings = []
        file_path = Path(file_path)
        
        if not file_path.exists():
            self.errors.append(LookMLValidationError(f"File not found: {file_path}"))
            return False, self.errors, self.warnings
        
        if not file_path.suffix == '.lkml':
            self.errors.append(LookMLValidationError(f"File must have .lkml extension: {file_path}"))
            return False, self.errors, self.warnings
        
        try:
            # Parse the LookML file using the lkml library
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Basic syntax validation using lkml parser
            try:
                parsed = lkml.load(content)
            except Exception as e:
                self.errors.append(LookMLValidationError(
                    f"LookML syntax error: {str(e)}", 
                    file_path=str(file_path)
                ))
                return False, self.errors, self.warnings
            
            # Validate structure and semantics
            self._validate_structure(parsed, str(file_path))
            
        except Exception as e:
            self.errors.append(LookMLValidationError(
                f"Validation failed: {str(e)}", 
                file_path=str(file_path)
            ))
            return False, self.errors, self.warnings
        
        return len(self.errors) == 0, self.errors, self.warnings
    
    def _validate_structure(self, parsed_content: Dict, file_path: str) -> None:
        """Validate the overall structure of the LookML content."""
        # Check for views
        if 'view' in parsed_content:
            views = parsed_content['view'] if isinstance(parsed_content['view'], list) else [parsed_content['view']]
            for view in views:
                self._validate_view(view, file_path)
        
        # Check for explores
        if 'explore' in parsed_content:
            explores = parsed_content['explore'] if isinstance(parsed_content['explore'], list) else [parsed_content['explore']]
            for explore in explores:
                self._validate_explore(explore, file_path)
    
    def _validate_view(self, view: Dict, file_path: str) -> None:
        """Validate a view definition."""
        view_name = view.get('name', 'unnamed_view')
        
        # Validate view name
        if not self._is_valid_identifier(view_name):
            self.errors.append(LookMLValidationError(
                f"Invalid view name '{view_name}': must be alphanumeric with underscores",
                file_path=file_path
            ))
        
        # Check for sql_table_name (required for most views)
        if 'sql_table_name' not in view and 'sql' not in view:
            self.warnings.append(f"View '{view_name}' has no sql_table_name or sql parameter")
        
        # Validate dimensions
        if 'dimension' in view:
            dimensions = view['dimension'] if isinstance(view['dimension'], list) else [view['dimension']]
            for dimension in dimensions:
                self._validate_dimension(dimension, view_name, file_path)
        
        # Validate dimension groups
        if 'dimension_group' in view:
            dimension_groups = view['dimension_group'] if isinstance(view['dimension_group'], list) else [view['dimension_group']]
            for dimension_group in dimension_groups:
                self._validate_dimension_group(dimension_group, view_name, file_path)
        
        # Validate measures
        if 'measure' in view:
            measures = view['measure'] if isinstance(view['measure'], list) else [view['measure']]
            for measure in measures:
                self._validate_measure(measure, view_name, file_path)
        
        # Validate drill_fields
        if 'drill_fields' in view:
            self._validate_drill_fields(view['drill_fields'], view_name, file_path)
    
    def _validate_dimension(self, dimension: Dict, view_name: str, file_path: str) -> None:
        """Validate a dimension definition."""
        dim_name = dimension.get('name', 'unnamed_dimension')
        
        # Validate dimension name
        if not self._is_valid_identifier(dim_name):
            self.errors.append(LookMLValidationError(
                f"Invalid dimension name '{dim_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Validate type
        dim_type = dimension.get('type')
        if dim_type and dim_type not in self.VALID_DIMENSION_TYPES:
            self.errors.append(LookMLValidationError(
                f"Invalid dimension type '{dim_type}' for dimension '{dim_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Check for SQL (usually required unless it's a derived dimension)
        if 'sql' not in dimension and dim_type != 'number':
            self.warnings.append(f"Dimension '{dim_name}' in view '{view_name}' has no SQL definition")
        
        # Validate group_label and group_item_label consistency
        has_group_label = 'group_label' in dimension
        has_group_item_label = 'group_item_label' in dimension
        if has_group_label != has_group_item_label:
            self.warnings.append(
                f"Dimension '{dim_name}' in view '{view_name}' has only one of group_label/group_item_label"
            )
    
    def _validate_dimension_group(self, dimension_group: Dict, view_name: str, file_path: str) -> None:
        """Validate a dimension group definition."""
        dg_name = dimension_group.get('name', 'unnamed_dimension_group')
        
        # Validate dimension group name
        if not self._is_valid_identifier(dg_name):
            self.errors.append(LookMLValidationError(
                f"Invalid dimension group name '{dg_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Validate type (required)
        dg_type = dimension_group.get('type')
        if not dg_type:
            self.errors.append(LookMLValidationError(
                f"Dimension group '{dg_name}' in view '{view_name}' missing required 'type' parameter",
                file_path=file_path
            ))
        elif dg_type not in self.VALID_DIMENSION_GROUP_TYPES:
            self.errors.append(LookMLValidationError(
                f"Invalid dimension group type '{dg_type}' for '{dg_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Validate timeframes for time dimension groups
        if dg_type == 'time':
            timeframes = dimension_group.get('timeframes', [])
            if timeframes:
                for timeframe in timeframes:
                    if timeframe not in self.VALID_TIMEFRAMES:
                        self.errors.append(LookMLValidationError(
                            f"Invalid timeframe '{timeframe}' in dimension group '{dg_name}' in view '{view_name}'",
                            file_path=file_path
                        ))
        
        # Validate datatype
        datatype = dimension_group.get('datatype')
        if datatype and datatype not in self.VALID_DATATYPES:
            self.errors.append(LookMLValidationError(
                f"Invalid datatype '{datatype}' in dimension group '{dg_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Check for SQL (required)
        if 'sql' not in dimension_group:
            self.errors.append(LookMLValidationError(
                f"Dimension group '{dg_name}' in view '{view_name}' missing required 'sql' parameter",
                file_path=file_path
            ))
    
    def _validate_measure(self, measure: Dict, view_name: str, file_path: str) -> None:
        """Validate a measure definition."""
        measure_name = measure.get('name', 'unnamed_measure')
        
        # Validate measure name
        if not self._is_valid_identifier(measure_name):
            self.errors.append(LookMLValidationError(
                f"Invalid measure name '{measure_name}' in view '{view_name}'",
                file_path=file_path
            ))
        
        # Validate type (required)
        measure_type = measure.get('type')
        if not measure_type:
            self.errors.append(LookMLValidationError(
                f"Measure '{measure_name}' in view '{view_name}' missing required 'type' parameter",
                file_path=file_path
            ))
        elif measure_type not in self.VALID_MEASURE_TYPES:
            self.errors.append(LookMLValidationError(
                f"Invalid measure type '{measure_type}' for measure '{measure_name}' in view '{view_name}'",
                file_path=file_path
            ))
    
    def _validate_explore(self, explore: Dict, file_path: str) -> None:
        """Validate an explore definition."""
        explore_name = explore.get('name', 'unnamed_explore')
        
        # Validate explore name
        if not self._is_valid_identifier(explore_name):
            self.errors.append(LookMLValidationError(
                f"Invalid explore name '{explore_name}'",
                file_path=file_path
            ))
        
        # Validate joins
        if 'join' in explore:
            joins = explore['join'] if isinstance(explore['join'], list) else [explore['join']]
            for join in joins:
                self._validate_join(join, explore_name, file_path)
    
    def _validate_join(self, join: Dict, explore_name: str, file_path: str) -> None:
        """Validate a join definition."""
        join_name = join.get('name', 'unnamed_join')
        
        # Validate join name
        if not self._is_valid_identifier(join_name):
            self.errors.append(LookMLValidationError(
                f"Invalid join name '{join_name}' in explore '{explore_name}'",
                file_path=file_path
            ))
        
        # Check for SQL (required)
        if 'sql' not in join and 'sql_on' not in join:
            self.errors.append(LookMLValidationError(
                f"Join '{join_name}' in explore '{explore_name}' missing required 'sql' or 'sql_on' parameter",
                file_path=file_path
            ))
        
        # Validate relationship
        relationship = join.get('relationship')
        if relationship and relationship not in self.VALID_RELATIONSHIP_TYPES:
            self.errors.append(LookMLValidationError(
                f"Invalid relationship '{relationship}' in join '{join_name}' in explore '{explore_name}'",
                file_path=file_path
            ))
    
    def _validate_drill_fields(self, drill_fields: List[str], view_name: str, file_path: str) -> None:
        """Validate drill_fields definition."""
        if not isinstance(drill_fields, list):
            self.errors.append(LookMLValidationError(
                f"drill_fields in view '{view_name}' must be a list",
                file_path=file_path
            ))
            return
        
        for field in drill_fields:
            if not isinstance(field, str) or not self._is_valid_identifier(field):
                self.errors.append(LookMLValidationError(
                    f"Invalid drill field '{field}' in view '{view_name}'",
                    file_path=file_path
                ))
    
    def _is_valid_identifier(self, name: str) -> bool:
        """Check if a name is a valid LookML identifier."""
        if not name or not isinstance(name, str):
            return False
        # LookML identifiers can contain letters, numbers, and underscores
        # They cannot start with a number
        return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name) is not None


def validate_lookml_file(file_path: Union[str, Path]) -> Tuple[bool, List[LookMLValidationError], List[str]]:
    """Convenience function to validate a single LookML file.
    
    Args:
        file_path: Path to the .view.lkml file to validate
        
    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    validator = LookMLValidator()
    return validator.validate_file(file_path)


def validate_lookml_directory(directory_path: Union[str, Path]) -> Dict[str, Tuple[bool, List[LookMLValidationError], List[str]]]:
    """Validate all .lkml files in a directory.
    
    Args:
        directory_path: Path to directory containing .lkml files
        
    Returns:
        Dictionary mapping file paths to validation results
    """
    directory_path = Path(directory_path)
    results = {}
    validator = LookMLValidator()
    
    for lkml_file in directory_path.glob('**/*.lkml'):
        results[str(lkml_file)] = validator.validate_file(lkml_file)
    
    return results
