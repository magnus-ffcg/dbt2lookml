"""Tests for the LookML validator."""

import pytest
from pathlib import Path
from dbt2lookml.validator import LookMLValidator, LookMLValidationError, validate_lookml_file


class TestLookMLValidator:
    """Test cases for LookML validator functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = LookMLValidator()
        self.test_files_dir = Path(__file__).parent / "expected"
    
    def test_validate_valid_lookml_file(self):
        """Test validation of a valid LookML file."""
        # Test with existing expected output file
        test_file = self.test_files_dir / "sales_waste.view.lkml"
        if test_file.exists():
            is_valid, errors, warnings = self.validator.validate_file(test_file)
            assert is_valid, f"Expected valid file but got errors: {[e.message for e in errors]}"
            assert len(errors) == 0
    
    def test_validate_complex_lookml_file(self):
        """Test validation of a complex LookML file with nested structures."""
        test_file = self.test_files_dir / "dq_icasoi_current.view.lkml"
        if test_file.exists():
            is_valid, errors, warnings = self.validator.validate_file(test_file)
            assert is_valid, f"Expected valid file but got errors: {[e.message for e in errors]}"
            assert len(errors) == 0
    
    def test_validate_file_with_explore(self):
        """Test validation of a file containing both views and explores."""
        test_file = self.test_files_dir / "sales_waste_with_explore.view.lkml"
        if test_file.exists():
            is_valid, errors, warnings = self.validator.validate_file(test_file)
            assert is_valid, f"Expected valid file but got errors: {[e.message for e in errors]}"
            assert len(errors) == 0
    
    def test_validate_nonexistent_file(self):
        """Test validation of a non-existent file."""
        is_valid, errors, warnings = self.validator.validate_file("nonexistent.lkml")
        assert not is_valid
        assert len(errors) == 1
        assert "File not found" in errors[0].message
    
    def test_validate_non_lkml_file(self):
        """Test validation of a file without .lkml extension."""
        # Create a temporary file with .txt extension
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("test content")
            temp_path = f.name
        
        try:
            is_valid, errors, warnings = self.validator.validate_file(temp_path)
            assert not is_valid
            assert len(errors) == 1
            assert "must have .lkml extension" in errors[0].message
        finally:
            os.unlink(temp_path)
    
    def test_is_valid_identifier(self):
        """Test identifier validation."""
        # Valid identifiers
        assert self.validator._is_valid_identifier("valid_name")
        assert self.validator._is_valid_identifier("ValidName")
        assert self.validator._is_valid_identifier("name123")
        assert self.validator._is_valid_identifier("_private")
        
        # Invalid identifiers
        assert not self.validator._is_valid_identifier("123invalid")
        assert not self.validator._is_valid_identifier("invalid-name")
        assert not self.validator._is_valid_identifier("invalid.name")
        assert not self.validator._is_valid_identifier("")
        assert not self.validator._is_valid_identifier(None)
    
    def test_validate_dimension_types(self):
        """Test dimension type validation."""
        valid_dimension = {
            'name': 'test_dim',
            'type': 'string',
            'sql': '${TABLE}.test_field'
        }
        self.validator._validate_dimension(valid_dimension, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Test invalid type
        self.validator.errors = []
        invalid_dimension = {
            'name': 'test_dim',
            'type': 'invalid_type',
            'sql': '${TABLE}.test_field'
        }
        self.validator._validate_dimension(invalid_dimension, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "Invalid dimension type" in self.validator.errors[0].message
    
    def test_validate_dimension_group_types(self):
        """Test dimension group type validation."""
        valid_dimension_group = {
            'name': 'test_dg',
            'type': 'time',
            'timeframes': ['raw', 'date', 'week'],
            'sql': '${TABLE}.test_date'
        }
        self.validator._validate_dimension_group(valid_dimension_group, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Test missing type
        self.validator.errors = []
        invalid_dimension_group = {
            'name': 'test_dg',
            'sql': '${TABLE}.test_date'
        }
        self.validator._validate_dimension_group(invalid_dimension_group, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "missing required 'type' parameter" in self.validator.errors[0].message
    
    def test_validate_measure_types(self):
        """Test measure type validation."""
        valid_measure = {
            'name': 'test_measure',
            'type': 'count'
        }
        self.validator._validate_measure(valid_measure, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Test missing type
        self.validator.errors = []
        invalid_measure = {
            'name': 'test_measure'
        }
        self.validator._validate_measure(invalid_measure, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "missing required 'type' parameter" in self.validator.errors[0].message
    
    def test_validate_timeframes(self):
        """Test timeframe validation for dimension groups."""
        # Valid timeframes
        valid_dimension_group = {
            'name': 'test_dg',
            'type': 'time',
            'timeframes': ['raw', 'date', 'week', 'month', 'quarter', 'year'],
            'sql': '${TABLE}.test_date'
        }
        self.validator._validate_dimension_group(valid_dimension_group, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Invalid timeframe
        self.validator.errors = []
        invalid_dimension_group = {
            'name': 'test_dg',
            'type': 'time',
            'timeframes': ['raw', 'invalid_timeframe'],
            'sql': '${TABLE}.test_date'
        }
        self.validator._validate_dimension_group(invalid_dimension_group, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "Invalid timeframe" in self.validator.errors[0].message
    
    def test_validate_join_relationships(self):
        """Test join relationship validation."""
        valid_join = {
            'name': 'test_join',
            'sql': 'LEFT JOIN test_table ON condition',
            'relationship': 'one_to_many'
        }
        self.validator._validate_join(valid_join, 'test_explore', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Invalid relationship
        self.validator.errors = []
        invalid_join = {
            'name': 'test_join',
            'sql': 'LEFT JOIN test_table ON condition',
            'relationship': 'invalid_relationship'
        }
        self.validator._validate_join(invalid_join, 'test_explore', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "Invalid relationship" in self.validator.errors[0].message
    
    def test_validate_drill_fields(self):
        """Test drill_fields validation."""
        # Valid drill fields
        valid_drill_fields = ['field1', 'field2', 'field_3']
        self.validator._validate_drill_fields(valid_drill_fields, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 0
        
        # Invalid drill fields (not a list)
        self.validator.errors = []
        invalid_drill_fields = "not_a_list"
        self.validator._validate_drill_fields(invalid_drill_fields, 'test_view', 'test.lkml')
        assert len(self.validator.errors) == 1
        assert "must be a list" in self.validator.errors[0].message
    
    def test_group_label_consistency(self):
        """Test that group_label and group_item_label are used consistently."""
        # Both present - should not generate warning
        dimension_with_both = {
            'name': 'test_dim',
            'type': 'string',
            'sql': '${TABLE}.test_field',
            'group_label': 'Test Group',
            'group_item_label': 'Test Item'
        }
        self.validator._validate_dimension(dimension_with_both, 'test_view', 'test.lkml')
        assert len([w for w in self.validator.warnings if 'group_label' in w]) == 0
        
        # Only one present - should generate warning
        self.validator.warnings = []
        dimension_with_one = {
            'name': 'test_dim',
            'type': 'string',
            'sql': '${TABLE}.test_field',
            'group_label': 'Test Group'
        }
        self.validator._validate_dimension(dimension_with_one, 'test_view', 'test.lkml')
        assert len([w for w in self.validator.warnings if 'group_label' in w]) == 1


class TestLookMLValidationError:
    """Test cases for LookMLValidationError."""
    
    def test_error_message_formatting(self):
        """Test error message formatting with different parameters."""
        # Basic error
        error = LookMLValidationError("Test error")
        assert str(error) == "Error: Test error"
        
        # Error with line number
        error = LookMLValidationError("Test error", line_number=42)
        assert str(error) == "Line: 42 | Error: Test error"
        
        # Error with file path
        error = LookMLValidationError("Test error", file_path="/path/to/file.lkml")
        assert str(error) == "File: /path/to/file.lkml | Error: Test error"
        
        # Error with both
        error = LookMLValidationError("Test error", line_number=42, file_path="/path/to/file.lkml")
        assert str(error) == "File: /path/to/file.lkml | Line: 42 | Error: Test error"


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_validate_lookml_file_function(self):
        """Test the standalone validate_lookml_file function."""
        test_file = Path(__file__).parent / "expected" / "sales_waste.view.lkml"
        if test_file.exists():
            is_valid, errors, warnings = validate_lookml_file(test_file)
            assert isinstance(is_valid, bool)
            assert isinstance(errors, list)
            assert isinstance(warnings, list)
    
    def test_validate_nonexistent_file_function(self):
        """Test validation function with non-existent file."""
        is_valid, errors, warnings = validate_lookml_file("nonexistent.lkml")
        assert not is_valid
        assert len(errors) > 0
        assert isinstance(errors[0], LookMLValidationError)


# Integration test to validate all expected output files
class TestIntegrationValidation:
    """Integration tests to validate all expected LookML output files."""
    
    def test_validate_all_expected_files(self):
        """Validate all files in the expected directory."""
        expected_dir = Path(__file__).parent / "expected"
        if not expected_dir.exists():
            pytest.skip("Expected directory not found")
        
        validator = LookMLValidator()
        failed_files = []
        
        for lkml_file in expected_dir.glob("*.lkml"):
            is_valid, errors, warnings = validator.validate_file(lkml_file)
            if not is_valid:
                failed_files.append((str(lkml_file), errors))
        
        if failed_files:
            error_msg = "The following expected files failed validation:\n"
            for file_path, errors in failed_files:
                error_msg += f"\n{file_path}:\n"
                for error in errors:
                    error_msg += f"  - {error.message}\n"
            pytest.fail(error_msg)
