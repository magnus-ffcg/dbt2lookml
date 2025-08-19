#!/usr/bin/env python3
"""Test script for the unidecode-based safe_name function."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dbt2lookml.generators.utils import safe_name

def test_unidecode_safe_name():
    """Test the safe_name function with various Unicode inputs."""
    test_cases = [
        ("My Field Name", "My_Field_Name"),
        ("åäö-test@123", "aao_test_123"),
        ("user.email", "user_email"),
        ("Москва", "Moskva"),
        ("北京", "Bei_Jing"),
        ("café", "cafe"),
        ("naïve", "naive"),
        ("Zürich", "Zurich"),
        ("François", "Francois"),
        ("José María", "Jose_Maria"),
        ("", "unnamed_d41d8cd9"),
        ("123numeric", "field_123numeric"),
        ("special!@#$%chars", "special_chars"),
        ("___multiple___", "multiple"),
    ]
    
    print("Testing unidecode-based safe_name function:")
    print("=" * 70)
    
    for input_val, expected in test_cases:
        result = safe_name(input_val)
        status = "✓" if result == expected else "✗"
        print(f"{status} {input_val:15} -> {result:20} (expected: {expected})")
    
    print("=" * 70)

if __name__ == "__main__":
    test_unidecode_safe_name()
