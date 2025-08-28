"""
Test for generating LKML from fixtures and comparing with expected output
"""

import lkml
import pytest

from dbt2lookml.cli import Cli


class TestNestedLkmlGeneration:
    """Test LKML generation from fixtures with nested structures using CLI"""

    def test_generate_nested_lkml_from_fixtures(self):
        """Test generating LKML from fixtures using CLI approach"""

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/data",
                "--output-dir",
                "output/tests/",
                "--select",
                "dq_ICASOI_Current",
                "--skip-explore",
            ]
        )

        # Run the CLI command using parse and generate like integration tests
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/dq_icasoi_current.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            with open('output/tests/item_dataquality/dq_icasoi_current.view.lkml', 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements instead of exact equality
        self._compare_lkml_structures(expected, generated)

    def _compare_lkml_structures(self, expected, generated):
        """Compare LKML structures focusing on key elements rather than exact equality"""
        # Extract views from both structures
        expected_views = {view['name']: view for view in expected['views']}
        generated_views = {view['name']: view for view in generated['views']}

        # Check that all expected views exist
        for view_name in expected_views:
            assert view_name in generated_views, f"Missing view: {view_name}"

        # Compare main view structure
        main_view_name = 'dq_icasoi_current'
        if main_view_name in expected_views:
            expected_main = expected_views[main_view_name]
            generated_main = generated_views[main_view_name]

            # Check dimensions exist (names only, ignore order)
            expected_dims = {dim['name'] for dim in expected_main.get('dimensions', [])}
            generated_dims = {dim['name'] for dim in generated_main.get('dimensions', [])}

            # Validate all expected dimensions are present
            for dim_name in expected_dims:
                assert dim_name in generated_dims, f"Missing expected dimension: {dim_name}"

            # Key classification dimensions should exist
            classification_dims = {
                'classification__assortment__code',
                'classification__assortment__description',
                'classification__item_group__code',
                'classification__item_group__description',
            }

            for dim_name in classification_dims:
                assert dim_name in generated_dims, f"Missing classification dimension: {dim_name}"

            # Check cross-view contamination - nested dimensions shouldn't be in main view
            nested_dim_names = {
                'markings__marking__code',
                'markings__marking__description',
                'code',
                'description',
            }
            for nested_dim in nested_dim_names:
                if nested_dim in generated_dims:
                    # If it exists, it should be hidden
                    dim_obj = next(
                        (d for d in generated_main.get('dimensions', []) if d.get('name') == nested_dim),
                        None,
                    )
                    if dim_obj:
                        assert dim_obj.get('hidden') == 'yes', f"Nested dimension {nested_dim} should be hidden in main view"

            # Check SQL column references for classification dimensions
            for dim in generated_main.get('dimensions', []):
                if dim['name'].startswith('classification__'):
                    expected_sql = '${TABLE}.Classification.'
                    assert expected_sql in dim.get('sql', ''), f"Wrong SQL reference in {dim['name']}: {dim.get('sql')}"
                elif dim['name'] == 'classification':
                    # Struct dimension should be hidden when expanded
                    assert dim.get('hidden') == 'yes', "Classification struct dimension should be hidden when expanded"

            # Check dimension groups exist and have proper properties
            expected_dg_names = {dg['name'] for dg in expected_main.get('dimension_groups', [])}
            generated_dg_names = {dg['name'] for dg in generated_main.get('dimension_groups', [])}

            # At least some key dimension groups should exist
            key_dg_names = {'delivery_start'}
            for dg_name in key_dg_names:
                if dg_name in expected_dg_names:
                    assert dg_name in generated_dg_names, f"Missing dimension group: {dg_name}"

                    # Validate dimension group properties
                    dg_obj = next(
                        (dg for dg in generated_main.get('dimension_groups', []) if dg.get('name') == dg_name),
                        None,
                    )
                    if dg_obj:
                        assert 'timeframes' in dg_obj, f"Dimension group {dg_name} missing timeframes"
                        assert 'date' in dg_obj.get('timeframes', []), f"Dimension group {dg_name} missing date timeframe"

            # Check group labels for classification dimensions
            for dim in generated_main.get('dimensions', []):
                if dim['name'] == 'classification__assortment__code':
                    assert dim.get('group_label') == 'Classification Assortment', f"Wrong group_label for {dim['name']}"
                    assert dim.get('group_item_label') == 'Code', f"Wrong group_item_label for {dim['name']}"
                elif dim['name'] == 'classification__assortment__description':
                    assert dim.get('group_label') == 'Classification Assortment', f"Wrong group_label for {dim['name']}"
                    assert dim.get('group_item_label') == 'Description', f"Wrong group_item_label for {dim['name']}"

            # Check SQL table name
            assert generated_main.get('sql_table_name') == expected_main.get('sql_table_name'), "SQL table name mismatch"

        # Check nested views exist and validate their content
        nested_view_name = 'dq_icasoi_current__markings__marking'
        if nested_view_name in expected_views:
            assert nested_view_name in generated_views, f"Missing nested view: {nested_view_name}"

            # Validate nested view content
            nested_view = generated_views[nested_view_name]
            nested_dims = {dim['name'] for dim in nested_view.get('dimensions', [])}

            # Should contain nested-specific dimensions
            expected_nested_dims = {'markings__marking__code', 'markings__marking__description'}
            for nested_dim in expected_nested_dims:
                assert nested_dim in nested_dims, f"Missing dimension in nested view: {nested_dim}"

            # Should NOT contain main view dimensions
            main_view_dims = {'classification__assortment__code', 'buying_item_gtin'}
            for main_dim in main_view_dims:
                assert main_dim not in nested_dims, f"Main view dimension {main_dim} should not be in nested view"

            # Should NOT contain dimension groups
            nested_dg_count = len(nested_view.get('dimension_groups', []))
            assert nested_dg_count == 0, f"Nested view should not contain dimension groups, found {nested_dg_count}"

        # Validate view count completeness
        expected_view_count = len(expected_views)
        generated_view_count = len(generated_views)
        assert (
            generated_view_count >= expected_view_count
        ), f"Missing views: expected {expected_view_count}, got {generated_view_count}"

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
                "dq_ICASOI_Current",
                # Note: NOT using --skip-explore to enable explore generation
            ]
        )

        # Run the CLI command using parse and generate like integration tests
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/dq_icasoi_current_with_explore.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            with open('output/tests/item_dataquality/dq_icasoi_current.view.lkml', 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_lkml_structures_with_explore(expected, generated)

    def _compare_lkml_structures_with_explore(self, expected, generated):
        """Compare LKML structures including explore functionality."""
        # First run the standard view comparison
        self._compare_lkml_structures(expected, generated)

        # Check that explore is generated
        expected_explores = expected.get('explores', [])
        generated_explores = generated.get('explores', [])

        if expected_explores:
            assert len(generated_explores) > 0, "Expected explore to be generated but none found"

            # Check main explore exists
            expected_explore = expected_explores[0]
            generated_explore = generated_explores[0]

            expected_name = expected_explore['name']
            generated_name = generated_explore['name']
            assert expected_name == generated_name, f"Explore name mismatch: expected {expected_name}, got {generated_name}"

            # Check explore has joins for nested views
            expected_joins = expected_explore.get('joins', [])
            generated_joins = generated_explore.get('joins', [])

            expected_join_names = {join['name'] for join in expected_joins}
            generated_join_names = {join['name'] for join in generated_joins}

            for join_name in expected_join_names:
                assert join_name in generated_join_names, f"Missing join in explore: {join_name}"

            # Validate join properties for nested views
            for expected_join in expected_joins:
                generated_join = next((j for j in generated_joins if j['name'] == expected_join['name']), None)
                assert generated_join is not None, f"Missing join: {expected_join['name']}"

                expected_rel = expected_join.get('relationship')
                generated_rel = generated_join.get('relationship')
                assert generated_rel == expected_rel, f"Join relationship mismatch for {expected_join['name']}"
                assert 'UNNEST' in generated_join.get('sql', ''), f"Join SQL should contain UNNEST for {expected_join['name']}"

    def test_generate_sales_waste_lkml(self):
        """Test generating LKML for sales waste model from real samples without explore"""

        # Initialize and run CLI
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
                "--skip-explore",
            ]
        )

        # Run the CLI command using parse and generate
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/sales_waste.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            output_path = (
                'output/tests/conlaybi/consumer_sales_secure_versioned/'
                'conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day.view.lkml'
            )
            with open(output_path, 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements
        self._compare_sales_waste_lkml_structures(expected, generated)

    def test_generate_sales_waste_lkml_with_explore(self):
        """Test generating LKML for sales waste model with explore functionality"""
        directory = 'output/tests/conlaybi/consumer_sales_secure_versioned'
        name = 'conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day'

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
                # Note: NOT using --skip-explore to enable explore generation
            ]
        )

        # Run the CLI command using parse and generate
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/sales_waste_with_explore.view.lkml', 'r') as file1:
            expected = lkml.load(file1)

            with open(f'{directory}/{name}.view.lkml', 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements including explore
        self._compare_sales_waste_lkml_structures_with_explore(expected, generated)

    def _compare_sales_waste_lkml_structures(self, expected, generated):
        """Compare sales waste LKML structures focusing on key elements"""
        # Extract views from both structures
        expected_views = {view['name']: view for view in expected['views']}
        generated_views = {view['name']: view for view in generated['views']}

        # Map expected view names to generated view names (different prefixes)
        view_name_mapping = {
            'f_store_sales_waste_day_v1': ('conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day'),
            'f_store_sales_waste_day_v1__waste': ('conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__waste'),
            'f_store_sales_waste_day_v1__sales': ('conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__sales'),
            'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys': (
                'conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__sales__f_sale_receipt_pseudo_keys'
            ),
        }

        # Check that all expected views exist (using mapping)
        for expected_name in expected_views:
            generated_name = view_name_mapping.get(expected_name, expected_name)
            assert generated_name in generated_views, f"Missing view: {expected_name} (expected as {generated_name})"

        # Compare main view structure
        main_view_name = 'f_store_sales_waste_day_v1'
        generated_main_name = view_name_mapping[main_view_name]
        if main_view_name in expected_views:
            expected_main = expected_views[main_view_name]
            generated_main = generated_views[generated_main_name]

            # Check dimensions exist (names only, ignore order)
            expected_dims = {dim['name'] for dim in expected_main.get('dimensions', [])}
            generated_dims = {dim['name'] for dim in generated_main.get('dimensions', [])}

            # Validate all expected dimensions are present
            for dim_name in expected_dims:
                assert dim_name in generated_dims, f"Missing expected dimension: {dim_name}"

            # Check that nested array dimensions are hidden in main view
            nested_dims = {'sales', 'waste'}
            for nested_dim in nested_dims:
                if nested_dim in generated_dims:
                    dim_obj = next(
                        (d for d in generated_main.get('dimensions', []) if d.get('name') == nested_dim),
                        None,
                    )
                    if dim_obj:
                        assert dim_obj.get('hidden') == 'yes', f"Nested dimension {nested_dim} should be hidden in main view"

            # Check dimension groups exist and have proper properties
            expected_dg_names = {dg['name'] for dg in expected_main.get('dimension_groups', [])}
            generated_dg_names = {dg['name'] for dg in generated_main.get('dimension_groups', [])}

            # Key dimension groups should exist
            key_dg_names = {'d', 'md_insert_dttm'}
            for dg_name in key_dg_names:
                if dg_name in expected_dg_names:
                    assert dg_name in generated_dg_names, f"Missing dimension group: {dg_name}"

            # Check SQL table name (allow for different backtick formatting)
            expected_table = expected_main.get('sql_table_name', '').replace('`', '')
            generated_table = generated_main.get('sql_table_name', '').replace('`', '')
            assert expected_table == generated_table, (
                f"SQL table name mismatch: expected {expected_main.get('sql_table_name')}, "
                f"got {generated_main.get('sql_table_name')}"
            )

        # Check nested views exist and validate their content
        nested_view_names = [
            'f_store_sales_waste_day_v1__waste',
            'f_store_sales_waste_day_v1__sales',
            'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys',
        ]
        for nested_view_name in nested_view_names:
            if nested_view_name in expected_views:
                generated_nested_name = view_name_mapping.get(nested_view_name, nested_view_name)
                assert generated_nested_name in generated_views, (
                    f"Missing nested view: {nested_view_name} " f"(expected as {generated_nested_name})"
                )

    def _compare_sales_waste_lkml_structures_with_explore(self, expected, generated):
        """Compare sales waste LKML structures including explore functionality."""
        # First run the standard view comparison
        self._compare_sales_waste_lkml_structures(expected, generated)

        # Check that explore is generated
        expected_explores = expected.get('explores', [])
        generated_explores = generated.get('explores', [])

        if expected_explores:
            assert len(generated_explores) > 0, "Expected explore to be generated but none found"

            # Check main explore exists (names will be different due to prefixing)
            expected_explore = expected_explores[0]
            generated_explore = generated_explores[0]

            # Just check that the generated explore name contains the base name
            expected_base_name = 'f_store_sales_waste_day'
            assert expected_base_name in generated_explore['name'], (
                f"Generated explore name should contain {expected_base_name}, " f"got {generated_explore['name']}"
            )

            # Check explore has joins for nested views
            expected_joins = expected_explore.get('joins', [])
            generated_joins = generated_explore.get('joins', [])

            # Map join names like we did for views
            join_name_mapping = {
                'f_store_sales_waste_day_v1__waste': ('conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__waste'),
                'f_store_sales_waste_day_v1__sales': ('conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__sales'),
                'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys': (
                    'conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day__sales__f_sale_receipt_pseudo_keys'
                ),
            }

            expected_join_names = {join['name'] for join in expected_joins}
            generated_join_names = {join['name'] for join in generated_joins}

            for expected_join_name in expected_join_names:
                mapped_join_name = join_name_mapping.get(expected_join_name, expected_join_name)
                assert mapped_join_name in generated_join_names, (
                    f"Missing join in explore: {expected_join_name} " f"(expected as {mapped_join_name})"
                )

            # Validate join properties for nested views
            for expected_join in expected_joins:
                mapped_join_name = join_name_mapping.get(expected_join['name'], expected_join['name'])
                generated_join = next((j for j in generated_joins if j['name'] == mapped_join_name), None)
                assert generated_join is not None, f"Missing join: {expected_join['name']} (expected as {mapped_join_name})"

                # Check relationship and SQL
                assert generated_join.get('relationship') == expected_join.get(
                    'relationship'
                ), f"Join relationship mismatch for {expected_join['name']}"
                assert 'UNNEST' in generated_join.get('sql', ''), f"Join SQL should contain UNNEST for {expected_join['name']}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
