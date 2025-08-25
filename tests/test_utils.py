"""Tests for core utilities."""

import json
import os
import tempfile

import pytest

from dbt2lookml.exceptions import CliError
from dbt2lookml.utils import FileHandler, Sql


class TestFileHandler:
    def test_read_json_file(self):
        """Test reading a JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            json.dump({'test': 'data'}, f)
        handler = FileHandler()
        result = handler.read(f.name)
        os.unlink(f.name)
        assert result == {'test': 'data'}

    def test_read_text_file(self):
        """Test reading a text file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('test data')
            f.flush()
        handler = FileHandler()
        result = handler.read(f.name, is_json=False)
        os.unlink(f.name)
        assert result == 'test data'

    def test_read_nonexistent_file(self):
        """Test reading a file that doesn't exist."""
        handler = FileHandler()
        with pytest.raises(CliError):
            handler.read('nonexistent.file')

    def test_write_file(self):
        """Test writing to a file."""
        handler = FileHandler()
        with tempfile.NamedTemporaryFile(delete=False) as f:
            handler.write(f.name, 'test content')
            with open(f.name, 'r') as f2:
                content = f2.read()
        os.unlink(f.name)
        assert content == 'test content'


class TestSql:
    def test_validate_sql_valid(self):
        """Test validating valid SQL."""
        self.assert_sql_string('${TABLE}.column', '${view_name}.column', '${view_name}.column')

    def test_validate_sql_with_semicolons(self):
        """Test validating SQL with semicolons."""
        self.assert_sql_string('${TABLE}.column;;', '${TABLE}.column;; ', '${TABLE}.column')

    # TODO Rename this here and in `test_validate_sql_valid` and `test_validate_sql_with_semicolons`
    def assert_sql_string(self, arg0, arg1, arg2):
        sql = Sql()
        assert sql.validate_sql(arg0) == '${TABLE}.column'
        assert sql.validate_sql(arg1) == arg2

    def test_validate_sql_invalid(self):
        """Test validating invalid SQL."""
        sql = Sql()
        assert sql.validate_sql('column') is None  # No ${} syntax
        assert sql.validate_sql('') is None  # Empty string
