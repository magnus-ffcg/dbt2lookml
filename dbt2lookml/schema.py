"""Schema parsing functionality for BigQuery schema strings.

This module provides tools for parsing BigQuery schema strings into structured
field definitions. It handles nested structures (STRUCT) and complex types (ARRAY)
while maintaining the hierarchical relationships between fields.
"""

from dataclasses import dataclass
from typing import List

from contextlib import contextmanager
import re


@dataclass
class SchemaField:
    """Represents a field in the BigQuery schema with its name, type, and path."""

    name: str
    type_str: str
    path: List[str]
    inner_types: List[str] = None

    def __str__(self) -> str:
        """Returns the complete field representation as it would appear in a schema."""
        path_str = '.'.join(self.path + ([self.name] if self.name else []))
        return f"{path_str} {self.type_str}".strip()


class SchemaParser:
    """Parser for BigQuery schema strings that handles nested structures and complex types."""

    def __init__(self):
        self._fields: List[SchemaField] = []
        self._current_path: List[str] = []

    def _parse_inner_content(self, text: str) -> str:
        """Extracts content within angle brackets."""
        current = []
        level = 0

        for char in text:
            if char == '<':
                level += 1
            elif char == '>':
                level -= 1
                if level < 0:
                    break
            current.append(char)

        return ''.join(current).strip()

    def _split_fields(self, text: str) -> List[str]:
        """Splits content on top-level commas."""
        result = []
        current = []
        level = 0

        for char in text + ',':
            if char == '<':
                level += 1
            elif char == '>':
                level -= 1
            elif char == ',' and level == 0:
                if current:
                    result.append(''.join(current).strip())
                current = []
                continue
            current.append(char)

        return [f for f in result if f]

    def _normalize_type(self, type_str: str) -> str:
        """Normalizes type strings by removing precision/scale for numeric types."""
        if 'NUMERIC' in type_str:
            return re.sub(r'NUMERIC\(\d+,\s*\d+\)', 'NUMERIC', type_str)
        return type_str

    def _process_type(self, type_str: str) -> tuple[str, str, bool]:
        """Processes a type string to determine its structure.
        Returns: (inner_content, type_prefix, has_struct)"""
        type_str = self._normalize_type(type_str)
        if type_str.startswith('ARRAY<'):
            inner = self._parse_inner_content(type_str[6:])
            if inner.startswith('STRUCT<'):
                return self._parse_inner_content(inner[7:]), 'ARRAY', True
            return inner, inner, False
        if type_str.startswith('STRUCT<'):
            return self._parse_inner_content(type_str[7:]), 'STRUCT', True
        return type_str, type_str, False

    @contextmanager
    def _path_context(self, name: str):
        """Context manager for tracking field paths."""
        if name:
            self._current_path.append(name)
        try:
            yield
        finally:
            if name:
                self._current_path.pop()

    def _add_field(self, name: str, type_str: str, inner_types: List[str] = None):
        """Adds a field to the result list."""
        type_str = self._normalize_type(type_str)
        self._fields.append(
            SchemaField(
                name=name,
                type_str=type_str,
                path=self._current_path.copy(),
                inner_types=inner_types,
            )
        )

    def _process_fields(self, content: str):
        """Processes multiple field definitions."""
        for field in self._split_fields(content):
            name, type_str = field.split(' ', 1)
            inner, type_prefix, has_struct = self._process_type(type_str.strip())

            if has_struct:
                self._add_field(name, type_prefix)
                with self._path_context(name):
                    self._process_fields(inner)
            else:
                self._add_field(name, type_str)

    def parse(self, schema_str: str) -> List[str]:
        """Parses a BigQuery schema string into a list of field definitions."""
        self._fields = []
        self._current_path = []

        inner, type_prefix, has_struct = self._process_type(schema_str)
        if has_struct:
            self._process_fields(inner)
        else:
            self._fields.append(SchemaField(name="", type_str=type_prefix, path=[]))

        return sorted(str(field) for field in self._fields)
