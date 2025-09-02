"""Comprehensive unit tests for safe_name function."""

import pytest
from unittest.mock import patch

from dbt2lookml.generators.utils import safe_name


class TestSafeNameComprehensive:
    """Comprehensive test coverage for safe_name function."""
    
    def test_simple_alphanumeric_name_unchanged(self):
        """Test simple alphanumeric name remains unchanged."""
        result = safe_name("SimpleField123")
        assert result == "SimpleField123"
    
    def test_name_with_spaces_replaced_with_underscores(self):
        """Test spaces are replaced with underscores."""
        result = safe_name("My Field Name")
        assert result == "My_Field_Name"
    
    def test_name_with_hyphens_replaced_with_underscores(self):
        """Test hyphens are replaced with underscores."""
        result = safe_name("field-name-test")
        assert result == "field_name_test"
    
    def test_name_with_at_symbols_replaced_with_underscores(self):
        """Test @ symbols are replaced with underscores."""
        result = safe_name("field@test@name")
        assert result == "field_test_name"
    
    def test_name_with_mixed_separators(self):
        """Test mixed separators (spaces, hyphens, @) are all replaced."""
        result = safe_name("field name-test@value")
        assert result == "field_name_test_value"
    
    def test_unicode_characters_transliterated(self):
        """Test Unicode characters are transliterated to ASCII."""
        result = safe_name("åäö-test")
        assert result == "aao_test"
    
    def test_cyrillic_characters_transliterated(self):
        """Test Cyrillic characters are transliterated."""
        result = safe_name("Москва")
        assert result == "Moskva"
    
    def test_special_characters_removed(self):
        """Test special characters are removed/replaced."""
        result = safe_name("field!@#$%^&*()test")
        assert result == "field__test"
    
    def test_dots_preserved_for_nested_fields(self):
        """Test dots are preserved for nested field names."""
        result = safe_name("parent.child.field")
        assert result == "parent.child.field"
    
    def test_multiple_consecutive_underscores_cleaned(self):
        """Test multiple consecutive underscores are cleaned up."""
        result = safe_name("field___test____name")
        assert result == "field__test__name"
    
    def test_leading_underscores_stripped(self):
        """Test leading underscores are stripped."""
        result = safe_name("___field_name")
        assert result == "field_name"
    
    def test_trailing_underscores_stripped(self):
        """Test trailing underscores are stripped."""
        result = safe_name("field_name___")
        assert result == "field_name"
    
    def test_leading_and_trailing_underscores_stripped(self):
        """Test both leading and trailing underscores are stripped."""
        result = safe_name("___field_name___")
        assert result == "field_name"
    
    def test_empty_string_returns_error_hash(self):
        """Test empty string returns error with hash."""
        result = safe_name("")
        assert result.startswith("error_")
        assert len(result) == 14  # "error_" + 8 char hash
    
    def test_whitespace_only_string_returns_error_hash(self):
        """Test whitespace-only string returns error with hash."""
        result = safe_name("   ")
        assert result.startswith("error_")
        assert len(result) == 14
    
    def test_special_chars_only_string_returns_error_hash(self):
        """Test string with only special characters returns error with hash."""
        result = safe_name("!@#$%")
        assert result.startswith("error_")
        assert len(result) == 14
    
    def test_hash_uniqueness_for_different_inputs(self):
        """Test that different inputs produce different hashes."""
        result1 = safe_name("")
        result2 = safe_name("   ")
        result3 = safe_name("!@#")
        
        # All should start with "error_" but have different hashes
        assert result1.startswith("error_")
        assert result2.startswith("error_")
        assert result3.startswith("error_")
        assert result1 != result2 != result3
    
    def test_hash_consistency_for_same_input(self):
        """Test that same input produces consistent hash."""
        result1 = safe_name("")
        result2 = safe_name("")
        assert result1 == result2
    
    def test_complex_unicode_mixed_with_ascii(self):
        """Test complex Unicode mixed with ASCII characters."""
        result = safe_name("Café-München@123")
        assert result == "Cafe_Munchen_123"
    
    def test_parentheses_and_brackets_replaced(self):
        """Test parentheses and brackets are replaced with underscores."""
        result = safe_name("field(test)[array]")
        assert result == "field_test__array"
    
    def test_arithmetic_operators_replaced(self):
        """Test arithmetic operators are replaced with underscores."""
        result = safe_name("field+test-value*2/3")
        assert result == "field_test_value_2_3"
    
    def test_preserve_existing_underscores(self):
        """Test existing underscores are preserved appropriately."""
        result = safe_name("field_name_test")
        assert result == "field_name_test"
    
    def test_double_underscores_preserved(self):
        """Test double underscores are preserved."""
        result = safe_name("field__name__test")
        assert result == "field__name__test"
    
    def test_mixed_case_preserved(self):
        """Test mixed case is preserved."""
        result = safe_name("MyFieldName")
        assert result == "MyFieldName"
    
    def test_numbers_preserved(self):
        """Test numbers are preserved."""
        result = safe_name("field123test456")
        assert result == "field123test456"
    
    def test_complex_nested_field_name(self):
        """Test complex nested field name with dots."""
        result = safe_name("items.details.specifications.value")
        assert result == "items.details.specifications.value"
    
    def test_nested_field_with_special_chars(self):
        """Test nested field name with special characters."""
        result = safe_name("items.detail name.test-value")
        assert result == "items.detail_name.test_value"
    
    def test_regex_pattern_matching_behavior(self):
        """Test specific regex pattern behaviors."""
        # Test space-hyphen-at replacement
        result1 = safe_name("a b-c@d")
        assert result1 == "a_b_c_d"
        
        # Test invalid character removal
        result2 = safe_name("a!b#c$d")
        assert result2 == "a_b_c_d"
    
    def test_consecutive_underscore_cleanup_edge_cases(self):
        """Test edge cases in consecutive underscore cleanup."""
        # Three or more underscores become double underscores
        result1 = safe_name("a___b")
        assert result1 == "a__b"
        
        # Four underscores become double underscores
        result2 = safe_name("a____b")
        assert result2 == "a__b"
        
        # Mixed with other characters
        result3 = safe_name("a!!!b")  # ! becomes _ so a___b -> a__b
        assert result3 == "a__b"
    
    def test_single_character_inputs(self):
        """Test single character inputs."""
        assert safe_name("a") == "a"
        assert safe_name("1") == "1"
        assert safe_name("_").startswith("error_")  # Single underscore gets stripped, becomes empty
        assert safe_name(".") == "."
    
    def test_two_character_inputs(self):
        """Test two character inputs."""
        assert safe_name("ab") == "ab"
        assert safe_name("a.b") == "a.b"
        assert safe_name("a_b") == "a_b"
        assert safe_name("__").startswith("error_")  # Double underscore gets stripped, becomes empty
    
    def test_unidecode_import_and_usage(self):
        """Test that unidecode is properly imported and used."""
        # Test with characters that unidecode handles
        result = safe_name("naïve café")
        assert result == "naive_cafe"
    
    def test_md5_hash_generation(self):
        """Test MD5 hash generation for empty results."""
        with patch('hashlib.md5') as mock_md5:
            mock_hash = mock_md5.return_value
            mock_hash.hexdigest.return_value = "abcdef1234567890"
            
            result = safe_name("")
            
            mock_md5.assert_called_once_with("".encode('utf-8'))
            mock_hash.hexdigest.assert_called_once()
            assert result == "error_abcdef12"
    
    def test_re_module_usage(self):
        """Test that regex operations work correctly."""
        # Test the specific regex patterns used
        result = safe_name("test   multiple    spaces")
        assert result == "test_multiple_spaces"
        
        result2 = safe_name("test---hyphens")
        assert result2 == "test_hyphens"
    
    def test_edge_case_only_dots(self):
        """Test string with only dots."""
        result = safe_name("...")
        assert result == "..."
    
    def test_edge_case_dots_and_underscores(self):
        """Test string with dots and underscores."""
        result = safe_name("._._.")
        assert result == "._._."
    
    def test_very_long_input(self):
        """Test very long input string."""
        long_input = "a" * 1000 + " test " + "b" * 1000
        result = safe_name(long_input)
        expected = "a" * 1000 + "_test_" + "b" * 1000
        assert result == expected
