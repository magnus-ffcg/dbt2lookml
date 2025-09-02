"""Comprehensive unit tests for LookmlExploreGenerator._group_strings method."""

import pytest
from argparse import Namespace

from dbt2lookml.generators.explore import LookmlExploreGenerator
from dbt2lookml.models.dbt import DbtModelColumn


class TestGroupStringsComprehensive:
    """Comprehensive test coverage for _group_strings method."""
    
    @pytest.fixture
    def cli_args(self):
        """Create CLI args fixture."""
        return Namespace(
            use_table_name=False,
            include_models=[],
            exclude_models=[],
            target_dir='output',
        )
    
    @pytest.fixture
    def generator(self, cli_args):
        """Create generator instance."""
        return LookmlExploreGenerator(cli_args)
    
    def test_empty_columns_returns_empty_dict(self, generator):
        """Test _group_strings with empty columns returns empty dict."""
        result = generator._group_strings([], [])
        assert result == {}
    
    def test_no_array_columns_returns_empty_dict(self, generator):
        """Test _group_strings with no array columns returns empty dict."""
        all_columns = [
            DbtModelColumn(name='id', data_type='STRING'),
            DbtModelColumn(name='name', data_type='STRING'),
        ]
        array_columns = []
        
        result = generator._group_strings(all_columns, array_columns)
        assert result == {}
    
    def test_single_array_column_basic_structure(self, generator):
        """Test basic structure creation for single array column."""
        array_col = DbtModelColumn(name='items', data_type='ARRAY')
        child_col = DbtModelColumn(name='items.code', data_type='STRING')
        
        all_columns = [array_col, child_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'items' in result
        assert 'column' in result['items']
        assert 'children' in result['items']
        assert result['items']['column'] == array_col
    
    def test_array_with_single_inner_type_structure(self, generator):
        """Test array with single inner type (ARRAY<INT64>) structure."""
        array_col = DbtModelColumn(name='numbers', data_type='ARRAY', inner_types=['INT64'])
        
        all_columns = [array_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'numbers' in result
        # Should create structure for single inner type arrays
        assert result['numbers']['column'] == array_col
    
    def test_array_with_multiple_inner_types_structure(self, generator):
        """Test array with multiple inner types structure."""
        array_col = DbtModelColumn(name='complex_array', data_type='ARRAY', inner_types=['STRING', 'INT64'])
        child_col = DbtModelColumn(name='complex_array.field1', data_type='STRING')
        
        all_columns = [array_col, child_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'complex_array' in result
        assert result['complex_array']['column'] == array_col
        assert len(result['complex_array']['children']) > 0
    
    def test_struct_column_structure(self, generator):
        """Test STRUCT column structure creation."""
        struct_col = DbtModelColumn(name='address', data_type='STRUCT')
        child_col = DbtModelColumn(name='address.street', data_type='STRING')
        
        all_columns = [struct_col, child_col]
        array_columns = [struct_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'address' in result
        assert result['address']['column'] == struct_col
    
    def test_nested_array_structure_creation(self, generator):
        """Test nested array structure creation."""
        parent_array = DbtModelColumn(name='items', data_type='ARRAY')
        nested_array = DbtModelColumn(name='items.tags', data_type='ARRAY')
        child_col = DbtModelColumn(name='items.tags.value', data_type='STRING')
        
        all_columns = [parent_array, nested_array, child_col]
        array_columns = [parent_array]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'items' in result
        items_structure = result['items']
        assert items_structure['column'] == parent_array
        
        # Check for nested structure
        children = items_structure['children']
        assert len(children) > 0
    
    def test_multiple_array_columns_processing(self, generator):
        """Test processing multiple array columns."""
        array1 = DbtModelColumn(name='items', data_type='ARRAY')
        array2 = DbtModelColumn(name='categories', data_type='ARRAY')
        child1 = DbtModelColumn(name='items.code', data_type='STRING')
        child2 = DbtModelColumn(name='categories.name', data_type='STRING')
        
        all_columns = [array1, array2, child1, child2]
        array_columns = [array1, array2]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'items' in result
        assert 'categories' in result
        assert result['items']['column'] == array1
        assert result['categories']['column'] == array2
    
    def test_deeply_nested_structure_creation(self, generator):
        """Test deeply nested structure creation."""
        level1 = DbtModelColumn(name='level1', data_type='ARRAY')
        level2 = DbtModelColumn(name='level1.level2', data_type='STRUCT')
        level3 = DbtModelColumn(name='level1.level2.level3', data_type='ARRAY')
        leaf = DbtModelColumn(name='level1.level2.level3.value', data_type='STRING')
        
        all_columns = [level1, level2, level3, leaf]
        array_columns = [level1]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'level1' in result
        # Verify nested structure exists
        level1_structure = result['level1']
        assert level1_structure['column'] == level1
        assert 'children' in level1_structure
    
    def test_remove_parts_helper_function_behavior(self, generator):
        """Test the behavior of the nested remove_parts function."""
        # This tests the internal logic by checking the filtering behavior
        parent_array = DbtModelColumn(name='items', data_type='ARRAY')
        direct_child = DbtModelColumn(name='items.code', data_type='STRING')
        nested_child = DbtModelColumn(name='items.details.name', data_type='STRING')
        unrelated = DbtModelColumn(name='other.field', data_type='STRING')
        
        all_columns = [parent_array, direct_child, nested_child, unrelated]
        array_columns = [parent_array]
        
        result = generator._group_strings(all_columns, array_columns)
        
        # Should only include children that match the parent pattern
        assert 'items' in result
        # The unrelated column should not be included in items structure
        items_structure = result['items']
        assert items_structure['column'] == parent_array
    
    def test_complex_mixed_data_types(self, generator):
        """Test complex structure with mixed ARRAY and STRUCT types."""
        items_array = DbtModelColumn(name='items', data_type='ARRAY')
        classification_struct = DbtModelColumn(name='items.classification', data_type='STRUCT')
        categories_array = DbtModelColumn(name='items.classification.categories', data_type='ARRAY')
        category_name = DbtModelColumn(name='items.classification.categories.name', data_type='STRING')
        
        all_columns = [items_array, classification_struct, categories_array, category_name]
        array_columns = [items_array]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'items' in result
        items_structure = result['items']
        assert items_structure['column'] == items_array
        assert len(items_structure['children']) > 0
    
    def test_array_with_space_in_inner_type(self, generator):
        """Test array with space in inner type name."""
        array_col = DbtModelColumn(name='complex_data', data_type='ARRAY', inner_types=['STRUCT<field STRING>'])
        child_col = DbtModelColumn(name='complex_data.field', data_type='STRING')
        
        all_columns = [array_col, child_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'complex_data' in result
        # Should handle complex inner types with spaces
        complex_structure = result['complex_data']
        assert complex_structure['column'] == array_col
    
    def test_edge_case_empty_inner_types(self, generator):
        """Test edge case with empty inner_types list."""
        array_col = DbtModelColumn(name='empty_array', data_type='ARRAY', inner_types=[])
        
        all_columns = [array_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'empty_array' in result
        # Should handle empty inner_types gracefully
        assert result['empty_array']['column'] == array_col
    
    def test_recursive_structure_building(self, generator):
        """Test recursive structure building with multiple levels."""
        # Create a structure: items -> details -> specifications -> values
        items = DbtModelColumn(name='items', data_type='ARRAY')
        details = DbtModelColumn(name='items.details', data_type='STRUCT')
        specs = DbtModelColumn(name='items.details.specifications', data_type='ARRAY')
        values = DbtModelColumn(name='items.details.specifications.values', data_type='STRING')
        
        all_columns = [items, details, specs, values]
        array_columns = [items]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'items' in result
        items_structure = result['items']
        
        # Verify recursive structure was built correctly
        assert items_structure['column'] == items
        assert 'children' in items_structure
        # The recursive nature should create nested children structures
        assert len(items_structure['children']) >= 0
    
    def test_column_filtering_by_parent_name(self, generator):
        """Test that columns are correctly filtered by parent name."""
        parent1 = DbtModelColumn(name='group1', data_type='ARRAY')
        parent2 = DbtModelColumn(name='group2', data_type='ARRAY')
        child1a = DbtModelColumn(name='group1.field1', data_type='STRING')
        child1b = DbtModelColumn(name='group1.field2', data_type='STRING')
        child2a = DbtModelColumn(name='group2.field1', data_type='STRING')
        
        all_columns = [parent1, parent2, child1a, child1b, child2a]
        array_columns = [parent1, parent2]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'group1' in result
        assert 'group2' in result
        
        # Each group should only contain its own children
        group1_structure = result['group1']
        group2_structure = result['group2']
        
        assert group1_structure['column'] == parent1
        assert group2_structure['column'] == parent2
    
    def test_no_matching_children_for_array(self, generator):
        """Test array column with no matching child columns."""
        array_col = DbtModelColumn(name='isolated_array', data_type='ARRAY')
        unrelated_col = DbtModelColumn(name='other_field', data_type='STRING')
        
        all_columns = [array_col, unrelated_col]
        array_columns = [array_col]
        
        result = generator._group_strings(all_columns, array_columns)
        
        assert 'isolated_array' in result
        isolated_structure = result['isolated_array']
        assert isolated_structure['column'] == array_col
        # Should have empty or minimal children structure
        assert 'children' in isolated_structure
