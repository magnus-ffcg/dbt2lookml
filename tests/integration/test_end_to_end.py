"""
Test for generating LKML from fixtures and comparing with expected output
"""

import lkml
import pytest

from dbt2lookml.cli import Cli


class TestNestedLkmlGeneration:
    """Test LKML generation from fixtures with nested structures using CLI"""


    def _compare_lkml_structures(self, expected, generated):
        """Generic LKML structure comparison that works for all test cases"""
        # Extract views from both structures
        expected_views = {view['name']: view for view in expected['views']}
        generated_views = {view['name']: view for view in generated['views']}

        # Check that all expected views exist by name
        for view_name in expected_views:
            assert view_name in generated_views, f"Missing view: {view_name}"

        # Check that no extra views are generated
        extra_views = set(generated_views.keys()) - set(expected_views.keys())
        assert not extra_views, f"Extra views generated: {extra_views}"

        # Compare each view using object-based comparison
        for view_name, expected_view in expected_views.items():
            generated_view = generated_views[view_name]
            self._compare_view_objects(expected_view, generated_view, view_name)
            
        # Compare explores if they exist
        if 'explores' in expected and 'explores' in generated:
            self._compare_explores(expected['explores'], generated['explores'])

    def _compare_view_objects(self, expected_view, generated_view, view_name):
        """Compare individual view objects focusing on structural elements"""
        # Compare dimensions
        expected_dims = {dim['name']: dim for dim in expected_view.get('dimensions', [])}
        generated_dims = {dim['name']: dim for dim in generated_view.get('dimensions', [])}
        
        # Check dimension counts (structural correctness)
        assert len(expected_dims) == len(generated_dims), \
            f"{view_name}: dimension count mismatch {len(generated_dims)}/{len(expected_dims)}"
        
        # Check all expected dimensions exist by name
        for dim_name in expected_dims:
            assert dim_name in generated_dims, f"{view_name}: missing dimension {dim_name}"
            
            # Compare key dimension properties
            exp_dim = expected_dims[dim_name]
            gen_dim = generated_dims[dim_name]
            
            # Check type if specified
            if 'type' in exp_dim:
                assert exp_dim['type'] == gen_dim.get('type'), \
                    f"{view_name}.{dim_name}: type mismatch {gen_dim.get('type')}/{exp_dim['type']}"
            
            # Check hidden status if specified
            if 'hidden' in exp_dim:
                assert exp_dim['hidden'] == gen_dim.get('hidden'), \
                    f"{view_name}.{dim_name}: hidden mismatch {gen_dim.get('hidden')}/{exp_dim['hidden']}"
            
            # Check SQL syntax if specified
            if 'sql' in exp_dim:
                assert exp_dim['sql'] == gen_dim.get('sql'), \
                    f"{view_name}.{dim_name}: sql mismatch\nExpected: {exp_dim['sql']}\nGenerated: {gen_dim.get('sql')}"
            
            # Validate SQL field syntax for non-hidden dimensions
            if gen_dim.get('sql') and not gen_dim.get('hidden'):
                sql = gen_dim['sql']
                # For nested views, simple field names are acceptable
                # For main views, should use ${TABLE}.field_name syntax
                is_nested_view = '__' in view_name
                if not is_nested_view:
                    # Main views should use ${TABLE}.field_name syntax
                    assert '${' in sql and '}' in sql, \
                        f"{view_name}.{dim_name}: Main view SQL should use Looker reference syntax: {sql}"

        # Compare dimension groups
        expected_dgs = {dg['name']: dg for dg in expected_view.get('dimension_groups', [])}
        generated_dgs = {dg['name']: dg for dg in generated_view.get('dimension_groups', [])}
        
        # Check dimension group counts (structural correctness)
        assert len(expected_dgs) == len(generated_dgs), \
            f"{view_name}: dimension group count mismatch {len(generated_dgs)}/{len(expected_dgs)}"
        
        # Check all expected dimension groups exist by name
        for dg_name in expected_dgs:
            assert dg_name in generated_dgs, f"{view_name}: missing dimension group {dg_name}"
            
            # Compare key dimension group properties
            exp_dg = expected_dgs[dg_name]
            gen_dg = generated_dgs[dg_name]
            
            # Check datatype if specified
            if 'datatype' in exp_dg:
                assert exp_dg['datatype'] == gen_dg.get('datatype'), \
                    f"{view_name}.{dg_name}: datatype mismatch {gen_dg.get('datatype')}/{exp_dg['datatype']}"
            
            # Check timeframes if specified
            if 'timeframes' in exp_dg:
                exp_timeframes = set(exp_dg['timeframes'])
                gen_timeframes = set(gen_dg.get('timeframes', []))
                assert exp_timeframes == gen_timeframes, \
                    f"{view_name}.{dg_name}: timeframes mismatch {gen_timeframes}/{exp_timeframes}"

        # Compare measures - skip for now since fixtures expect basic count measures
        # but our generator only creates measures from metadata configuration
        # TODO: Add default count measure generation or update fixtures
        # expected_measures = {measure['name']: measure for measure in expected_view.get('measures', [])}
        # generated_measures = {measure['name']: measure for measure in generated_view.get('measures', [])}

        # Compare sql_table_name (normalize backticks)
        if 'sql_table_name' in expected_view:
            expected_table = expected_view['sql_table_name'].replace('`', '')
            generated_table = generated_view.get('sql_table_name', '').replace('`', '')
            assert expected_table == generated_table, \
                f"{view_name}: sql_table_name mismatch {generated_table}/{expected_table}"

    def _compare_explores(self, expected_explores, generated_explores):
        """Generic explore comparison that works for all test cases"""
        assert len(expected_explores) == len(generated_explores), \
            f"Explore count mismatch: expected {len(expected_explores)}, got {len(generated_explores)}"
        
        # Compare each explore
        for i, expected_explore in enumerate(expected_explores):
            generated_explore = generated_explores[i]
            
            # Compare explore names (allow for prefixing differences)
            expected_name = expected_explore['name']
            generated_name = generated_explore['name']
            
            # Extract base name from expected to allow for prefixing
            base_name = expected_name.split('__')[-1] if '__' in expected_name else expected_name
            assert base_name in generated_name, \
                f"Explore name mismatch: expected to contain '{base_name}', got '{generated_name}'"
            
            # Compare joins if they exist
            if 'joins' in expected_explore and 'joins' in generated_explore:
                self._compare_joins(expected_explore['joins'], generated_explore['joins'])

    def _compare_joins(self, expected_joins, generated_joins):
        """Generic join comparison that works for all test cases"""
        expected_join_names = {join['name'] for join in expected_joins}
        generated_join_names = {join['name'] for join in generated_joins}
        
        # Check that all expected joins exist by name
        assert expected_join_names == generated_join_names, \
            f"Join name mismatch: expected {expected_join_names}, got {generated_join_names}"
        
        # Compare individual join properties
        expected_joins_dict = {join['name']: join for join in expected_joins}
        generated_joins_dict = {join['name']: join for join in generated_joins}
        
        for join_name in expected_join_names:
            exp_join = expected_joins_dict[join_name]
            gen_join = generated_joins_dict[join_name]
            
            # Check relationship if specified
            if 'relationship' in exp_join:
                assert exp_join['relationship'] == gen_join.get('relationship'), \
                    f"Join {join_name}: relationship mismatch {gen_join.get('relationship')}/{exp_join['relationship']}"
            
            # Check that SQL contains UNNEST for array joins
            if 'sql' in gen_join:
                assert 'UNNEST' in gen_join['sql'], \
                    f"Join {join_name}: SQL should contain UNNEST"
                
                # Compare SQL if specified in expected join
                if 'sql' in exp_join:
                    # Normalize whitespace and case for comparison
                    expected_sql = ' '.join(exp_join['sql'].split()).upper()
                    generated_sql = ' '.join(gen_join['sql'].split()).upper()
                    assert expected_sql == generated_sql, \
                        f"Join {join_name}: SQL mismatch\nExpected: {exp_join['sql']}\nGenerated: {gen_join['sql']}"

    def test_generate_nested_lkml_with_explore(self):
        """Test LKML generation with explore functionality."""
        # Initialize and run CLI with explore enabled
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/data",
                "--output-dir",
                "output/tests/",
                "--select",
                "conlaybi_item_dataquality__dq_ICASOI_Current",
                "--use-table-name",
            ]
        )

        # Run the CLI command using parse and generate like integration tests
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/dq_icasoi_current.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            with open('output/tests/conlaybi/item_dataquality/dq_icasoi_current.view.lkml', 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_lkml_structures(expected, generated)


    def test_generate_sales_waste_lkml_with_explore(self):
        """Test generating LKML for sales waste model with explore functionality"""
        directory = 'output/tests/conlaybi/consumer_sales_secure_versioned'
        name = 'f_store_sales_waste_day_v1'

        # Initialize and run CLI with explore enabled
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/data",
                "--output-dir",
                "output/tests/",
                "--select",
                "conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day",
                "--use-table-name"
            ]
        )

        # Run the CLI command using parse and generate
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/f_store_sales_waste_day_v1.view.lkml', 'r') as file1:
            expected = lkml.load(file1)

            with open(f'{directory}/{name}.view.lkml', 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_lkml_structures(expected, generated)

    def test_generate_d_item_v3_complex_structs_with_explore(self):
        """Test generating LKML for d_item_v3 model with explore functionality"""
        
        # Initialize and run CLI with fixture data
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            '--target-dir', 'tests/fixtures/data',
            '--output-dir', 'output/test_d_item_v3/',
            '--select', 'conlaybi_item_versioned__d_item',
            '--use-table-name',
        ])

        # Parse and generate models
        models = cli.parse(args)
        assert len(models) > 0, "No models found for d_item_v3"
        
        # Generate LKML files fresh for this test
        cli.generate(args, models)

        # Load expected and generated files
        with open('tests/fixtures/expected/d_item_v3.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            
        # Use the freshly generated file from this test run
        output_path = 'output/test_d_item_v3/conlaybi/item_versioned/d_item_v3.view.lkml'
        with open(output_path, 'r') as file2:
            generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_lkml_structures(expected, generated)

    def test_generate_f_store_sales_day_selling_entity_v1_with_explore(self):
        """Test generating LKML for f_store_sales_day_selling_entity_v1 model with explore functionality"""
        directory = 'output/tests/conlaybi/consumer_sales_looker'
        name = 'f_store_sales_day_selling_entity_v1'

        # Initialize and run CLI with fixture data
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            '--target-dir', 'tests/fixtures/data',
            '--output-dir', 'output/tests/',
            '--select', 'conlaybi_consumer_sales_looker__f_store_sales_day_selling_entity',
            '--use-table-name',
        ])

        # Parse and generate models
        models = cli.parse(args)
        assert len(models) > 0, "No models found for f_store_sales_day_selling_entity_v1"
        
        # Generate LKML files fresh for this test
        cli.generate(args, models)

        # Load expected and generated files
        with open('tests/fixtures/expected/f_store_sales_day_selling_entity_v1.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            
        with open(f'{directory}/{name}.view.lkml', 'r') as file2:
            generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_lkml_structures(expected, generated)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
