"""Utility classes for file handling and SQL validation."""

import json
import logging
from pathlib import Path
from typing import Optional, Dict, Union

from dbt2lookml.exceptions import CliError


class FileHandler:
    """Handles file operations for reading and writing files."""

    def read(self, file_path: Union[str, Path], is_json: bool = True) -> Dict:
        """Load file from disk.

        Args:
            file_path: Path to the file to read
            is_json: If True, parse the file as JSON, otherwise read as text

        Returns:
            Dictionary containing the JSON data if is_json=True, otherwise raw text contents

        Raises:
            CliError: If the file cannot be found or read
        """
        path = Path(file_path)
        try:
            with path.open("r", encoding="utf-8") as f:
                raw_file = json.load(f) if is_json else f.read()
        except FileNotFoundError as e:
            msg = f"Could not find file at {path}."
            details = "Use --target-dir to change the search path for the manifest.json file."
            logging.error(f"{msg} {details}")
            raise CliError(msg, details) from e
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in file {path}"
            raise CliError(msg, str(e)) from e
        except Exception as e:
            msg = f"Error reading file {path}"
            raise CliError(msg, str(e)) from e

        return raw_file

    def write(self, file_path: Union[str, Path], contents: str) -> None:
        """Write contents to a file.

        Args:
            file_path: Path where to write the file
            contents: String contents to write to the file

        Raises:
            CliError: If the file cannot be written
        """
        path = Path(file_path)
        try:
            # Ensure directory exists
            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open("w", encoding="utf-8") as f:
                f.truncate()  # Clear file to allow overwriting
                f.write(contents)
        except Exception as e:
            msg = f"Could not write file at {path}"
            raise CliError(msg, str(e)) from e


class Sql:
    """Handles SQL validation and formatting."""

    def validate_sql(self, sql: str) -> Optional[str]:
        """Validate that a string is a valid Looker SQL expression.

        Args:
            sql: SQL expression to validate

        Returns:
            Validated and cleaned SQL expression, or None if invalid
        """
        if not sql:
            return None

        sql = sql.strip()

        if self._has_ending_semicolons(sql):
            logging.warning(
                f"SQL expression '{sql}' ends with semicolons. They will be removed as lkml adds "
                "them automatically."
            )
            sql = sql.rstrip(";").rstrip(";").strip()

        if not self._has_dollar_syntax(sql):
            logging.warning(
                f"SQL expression '{sql}' does not contain ${{TABLE}} or ${{view_name}} syntax"
            )
            return None

        return sql

    @staticmethod
    def _has_dollar_syntax(sql: str) -> bool:
        """Check if the string has ${TABLE}.example or ${view_name} syntax.

        Args:
            sql: SQL expression to check

        Returns:
            True if the SQL contains proper Looker syntax
        """
        return "${" in sql and "}" in sql

    @staticmethod
    def _has_ending_semicolons(sql: str) -> bool:
        """Check if the string ends with semicolons.

        Args:
            sql: SQL expression to check

        Returns:
            True if the SQL ends with semicolons
        """
        return sql.endswith(";;")
