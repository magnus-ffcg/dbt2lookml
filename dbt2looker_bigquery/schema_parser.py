from typing import List, Optional
import re
from dataclasses import dataclass
from . import looker

@dataclass
class SchemaField:
    """Represents a field in the BigQuery schema"""
    name: str
    type: str
    prefix: str = ""

    @property
    def full_name(self) -> str:
        """Returns the fully qualified field name with prefix"""
        return f"{self.prefix}.{self.name}" if self.prefix else self.name

    def __str__(self) -> str:
        """Returns the string representation of the field"""
        return f"{self.full_name} {self.type}"

class SchemaParser:
    """Parser for BigQuery schema strings"""
    
    # BigQuery types
    TYPES = [ k for k in looker.LOOKER_BIGQUERY_DTYPE_MAP.keys() if k not in ('STRUCT', 'ARRAY') ]
    
    def normalize_type(self, type_def: str) -> str:
        """
        Normalize type names to BigQuery format.
        
        Args:
            type_def: The type definition to normalize
            
        Returns:
            Normalized type string
        """
        # Handle numeric types (DECIMAL/NUMERIC)
        if "(" in type_def:
            base_type = type_def.split("(")[0].strip().upper()
            if base_type in SchemaParser.TYPES:
                return base_type
        
        type_def = type_def.upper()
        
        # Handle array types
        if type_def.startswith("ARRAY<"):
            inner_type = type_def[6:-1]  # Remove ARRAY< and >
            if inner_type.startswith("ARRAY<"):
                return f"ARRAY<{SchemaParser.normalize_type(inner_type)}>"
            elif inner_type.startswith("STRUCT<"):
                return "ARRAY<STRUCT>"
            else:
                return f"ARRAY<{SchemaParser.normalize_type(inner_type)}>"
        
        return type_def

    def parse_field(self,field_str: str, prefix: str = "") -> Optional[SchemaField]:
        """
        Parse a single field string into a SchemaField object.
        
        Args:
            field_str: The field string to parse
            prefix: Optional prefix for nested fields
            
        Returns:
            SchemaField object or None if parsing fails
        """
        field_str = field_str.strip()
        if " " not in field_str:
            return None
            
        # Extract field name
        name_match = re.match(r"(\w+)\s+", field_str)
        if not name_match:
            return None
            
        name = name_match.group(1)
        type_def = field_str[len(name):].strip()
        
        return SchemaField(name=name, type=type_def, prefix=prefix)

    def parse_struct(self, struct_str: str, prefix: str = "", flatten_arrays: bool = True) -> List[str]:
        """
        Parse a struct definition into a list of field strings.
        
        Args:
            struct_str: The struct string to parse
            prefix: Optional prefix for nested fields
            flatten_arrays: Whether to flatten nested array types
            
        Returns:
            List of field strings
        """
        try:
            # Remove struct<...> wrapper if present
            struct_str = struct_str.strip()
            if not struct_str:
                return []
                
            if struct_str.upper().startswith("STRUCT<"):
                if not struct_str.endswith(">"):
                    return []
                struct_str = struct_str[7:-1]
            
            result = []
            level = 0  # Track nesting level
            current = ""
            in_brackets = False
            
            # Parse character by character
            for char in struct_str + ",":  # Add comma to handle last field
                if char == "," and level == 0 and not in_brackets:
                    if current:
                        field = SchemaParser.parse_field(current, prefix)
                        if field:
                            if "STRUCT<" in field.type.upper():
                                # Handle struct types
                                array_depth = 0
                                type_def = field.type
                                
                                # Handle array of struct
                                while type_def.upper().startswith("ARRAY<"):
                                    array_depth += 1
                                    type_def = type_def[6:-1]
                                
                                # Add appropriate type based on nesting
                                if array_depth > 0:
                                    array_type = "ARRAY<ARRAY<STRUCT>>" if array_depth > 1 else "ARRAY<STRUCT>"
                                    result.append(f"{field.full_name} {array_type}")
                                    
                                    if array_depth > 1 and not flatten_arrays:
                                        result.append(f"{field.full_name} {SchemaParser.normalize_type(field.type)}")
                                        current = ""
                                        continue
                                else:
                                    result.append(f"{field.full_name} STRUCT")
                                
                                # Parse nested struct
                                result.extend(SchemaParser.parse_struct(type_def, field.full_name, flatten_arrays))
                            else:
                                # Handle basic types
                                result.append(f"{field.full_name} {SchemaParser.normalize_type(field.type)}")
                        current = ""
                else:
                    # Track nesting level
                    if char == "(":
                        in_brackets = True
                    elif char == ")":
                        in_brackets = False
                    elif char == "<":
                        level += 1
                    elif char == ">":
                        level -= 1
                    current += char
            
            return result
        except Exception:
            return []

    def parse(self, schema: str) -> List[str]:
        """
        Parse a nested schema string into a flat list of column paths and types.
        
        Args:
            schema: The schema string to parse
            
        Returns:
            List of strings in format "field_path type"
            
        Example:
            Input: "ARRAY<STRUCT<a INT64, b ARRAY<STRUCT<c STRING>>>>"
            Output: ["a INT64", "b ARRAY<STRUCT>", "b.c STRING"]
        """
        try:
            # Handle top-level array
            flatten_arrays = True
            if schema.upper().startswith("ARRAY<"):
                if not schema.endswith(">"):
                    return []
                schema = schema[6:-1]
                if schema.upper().startswith("ARRAY<"):
                    flatten_arrays = False
            
            return sorted(self.parse_struct(schema, flatten_arrays=flatten_arrays))
        except Exception:
            return []
