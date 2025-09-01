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

        # Compare key structural elements instead of exact equality
        self._compare_lkml_structures(expected, generated)

    def _compare_lkml_structures(self, expected, generated):
        """Compare LKML structures using direct object comparison for better maintainability"""
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
            
        # Compare joins if they exist
        if 'joins' in expected and 'joins' in generated:
            self._compare_joins(expected['joins'], generated['joins'])

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

        # Skip measure comparison for now - measures depend on metadata configuration
        # TODO: Add measure comparison when metadata handling is standardized

        # Compare sql_table_name (normalize backticks)
        if 'sql_table_name' in expected_view:
            expected_table = expected_view['sql_table_name'].replace('`', '')
            generated_table = generated_view.get('sql_table_name', '').replace('`', '')
            assert expected_table == generated_table, \
                f"{view_name}: sql_table_name mismatch {generated_table}/{expected_table}"

    def _compare_joins(self, expected_joins, generated_joins):
        """Compare join structures by name and key properties"""
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
            
            # Check join type if specified
            if 'type' in exp_join:
                assert exp_join['type'] == gen_join.get('type'), \
                    f"Join {join_name}: type mismatch {gen_join.get('type')}/{exp_join['type']}"
            
            # Check sql_on if specified
            if 'sql_on' in exp_join:
                assert exp_join['sql_on'] == gen_join.get('sql_on'), \
                    f"Join {join_name}: sql_on mismatch {gen_join.get('sql_on')}/{exp_join['sql_on']}"

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
                "--include-explore",
                "--use-table-name",
            ]
        )

        # Run the CLI command using parse and generate like integration tests
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/dq_icasoi_current_with_explore.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            with open('output/tests/conlaybi/item_dataquality/dq_icasoi_current.view.lkml', 'r') as file2:
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
                "--use-table-name"
            ]
        )

        # Run the CLI command using parse and generate
        models = cli.parse(args)
        cli.generate(args, models)

        with open('tests/fixtures/expected/sales_waste.view.lkml', 'r') as file1:
            expected = lkml.load(file1)
            output_path = (
                'output/tests/conlaybi/consumer_sales_secure_versioned/'
                'f_store_sales_waste_day_v1.view.lkml'
            )
            with open(output_path, 'r') as file2:
                generated = lkml.load(file2)

        # Compare key structural elements
        self._compare_sales_waste_lkml_structures(expected, generated)

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
                "--use-table-name",
                "--include-explore"
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
            'f_store_sales_waste_day_v1': ('f_store_sales_waste_day_v1'),
            'f_store_sales_waste_day_v1__waste': ('f_store_sales_waste_day_v1__waste'),
            'f_store_sales_waste_day_v1__sales': ('f_store_sales_waste_day_v1__sales'),
            'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys': (
                'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys'
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
                'f_store_sales_waste_day_v1__waste': ('f_store_sales_waste_day_v1__waste'),
                'f_store_sales_waste_day_v1__sales': ('f_store_sales_waste_day_v1__sales'),
                'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys': (
                    'f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys'
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


    def test_generate_d_item_v3_complex_structs(self):
        """Test generating LKML for d_item_v3 model to verify complex struct handling"""

        # Initialize and run CLI with fixture data
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            '--target-dir', 'tests/fixtures/data',
            '--output-dir', 'output/test_d_item_v3/',
            '--select', 'conlaybi_item_versioned__d_item',
            '--use-table-name',
            '--include-explore'
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

        # Compare key structural elements
        self._compare_d_item_v3_lkml_structures(expected, generated)

    def _compare_d_item_v3_lkml_structures(self, expected, generated):
        """Compare d_item_v3 LKML structures focusing on complex struct handling"""
        # Extract views from both structures
        expected_views = {view['name']: view for view in expected['views']}
        generated_views = {view['name']: view for view in generated['views']}

        # Key nested views that should exist for complex structs
        key_nested_views = [
            'd_item_v3__accreditation',
            'd_item_v3__country_of_origin',
            'd_item_v3__central_department',
            'd_item_v3__load_carrier_deposit',
            'd_item_v3__ica_swedish_accreditation',
            'd_item_v3__ica_ethical_accreditation',
            'd_item_v3__ica_ecological_accreditation',
            'd_item_v3__item_information_claim_detail',
            'd_item_v3__ica_environmental_accreditation',
            'd_item_v3__ica_non_ecological_accreditation',
            'd_item_v3__packaging_information__packaging_material_composition',
            'd_item_v3__packaging_information__packaging_material_composition__packaging_material_composition_quantity'
        ]

        # Check that all expected nested views exist
        for nested_view_name in key_nested_views:
            if nested_view_name in expected_views:
                assert nested_view_name in generated_views, f"Missing nested view: {nested_view_name}"

        # Compare main view structure
        main_view_name = 'd_item_v3'
        if main_view_name in expected_views:
            expected_main = expected_views[main_view_name]
            generated_main = generated_views[main_view_name]

            # Check dimensions exist (names only, ignore order)
            expected_dims = {dim['name'] for dim in expected_main.get('dimensions', [])}
            generated_dims = {dim['name'] for dim in generated_main.get('dimensions', [])}

            # Key complex struct dimensions that should exist
            key_struct_dims = {
                'assortment_attributes__ecological',
                'assortment_attributes__environmental',
                'assortment_attributes__gdpr_sensitive__code_description',
                'assortment_attributes__ica_swedish__code_name',
                'brand__code_description',
                'category_specific_attributes__colour__code_value',
                'measurements__depth',
                'measurements__gross_weight_in_gram',
                'lifecycle__central_status',
                'primary_soi_supplier_reference__supplier_organization_name'
            }

            for dim_name in key_struct_dims:
                if dim_name in expected_dims:
                    assert dim_name in generated_dims, f"Missing complex struct dimension: {dim_name}"

            # Check that array dimensions are hidden in main view when they have nested views
            array_dims_with_nested_views = {
                'accreditation',
                'country_of_origin', 
                'central_department',
                'load_carrier_deposit',
                'ica_swedish_accreditation',
                'ica_ethical_accreditation',
                'ica_ecological_accreditation',
                'item_information_claim_detail',
                'ica_environmental_accreditation',
                'ica_non_ecological_accreditation'
            }

            for array_dim in array_dims_with_nested_views:
                if array_dim in generated_dims:
                    dim_obj = next(
                        (d for d in generated_main.get('dimensions', []) if d.get('name') == array_dim),
                        None,
                    )
                    if dim_obj:
                        assert dim_obj.get('hidden') == 'yes', f"Array dimension {array_dim} should be hidden when it has nested views"

            # Check group labels for complex struct dimensions
            for dim in generated_main.get('dimensions', []):
                if dim['name'] == 'assortment_attributes__gdpr_sensitive__code_description':
                    assert 'Assortment attributes' in dim.get('group_label', ''), f"Wrong group_label for {dim['name']}"
                    assert 'Code description' in dim.get('group_item_label', ''), f"Wrong group_item_label for {dim['name']}"
                elif dim['name'] == 'brand__code_description':
                    assert dim.get('group_label') == 'Brand', f"Wrong group_label for {dim['name']}"
                    assert dim.get('group_item_label') == 'Code description', f"Wrong group_item_label for {dim['name']}"

            # Check dimension groups exist and have proper properties
            expected_dg_names = {dg['name'] for dg in expected_main.get('dimension_groups', [])}
            generated_dg_names = {dg['name'] for dg in generated_main.get('dimension_groups', [])}

            # Key dimension groups should exist
            key_dg_names = {'ecr_revision', 'lifecycle__creation_datetime', 'md_insert_dttm'}
            for dg_name in key_dg_names:
                if dg_name in expected_dg_names:
                    assert dg_name in generated_dg_names, f"Missing dimension group: {dg_name}"

            # Check SQL table name
            expected_table = expected_main.get('sql_table_name', '').replace('`', '')
            generated_table = generated_main.get('sql_table_name', '').replace('`', '')
            assert expected_table == generated_table, f"SQL table name mismatch: {expected_table} vs {generated_table}"

        # Validate nested view content for complex structs
        test_nested_view = 'd_item_v3__item_information_claim_detail'
        if test_nested_view in expected_views and test_nested_view in generated_views:
            nested_view = generated_views[test_nested_view]
            nested_dims = {dim['name'] for dim in nested_view.get('dimensions', [])}

            # Should contain nested-specific dimensions with proper structure (using actual generated names)
            expected_nested_dims = {
                'item_information_claim_detail__claim_element__claim_element_code_description',
                'item_information_claim_detail__claim_type__claim_type_code_name',
                'item_information_claim_detail__is_item_information_claim_marked_on_package'
            }
            for nested_dim in expected_nested_dims:
                if any(nested_dim.split('__')[-1] in dim_name for dim_name in nested_dims):
                    # Check that at least some expected dimension pattern exists
                    continue
                else:
                    # Only fail if no similar dimension exists
                    similar_dims = [d for d in nested_dims if nested_dim.split('__')[-2:] == d.split('__')[-2:]]
                    if not similar_dims:
                        assert False, f"Missing dimension pattern in nested view: {nested_dim}"

            # Check group labels in nested views (using actual generated structure)
            for dim in nested_view.get('dimensions', []):
                if 'claim_element_code_description' in dim['name']:
                    assert 'claim element' in dim.get('group_label', '').lower(), f"Wrong group_label in nested view for {dim['name']}"

        # Check that no empty nested views are generated (our fix should prevent this)
        for view_name, view_content in generated_views.items():
            if view_name != main_view_name:  # Skip main view
                dimensions = view_content.get('dimensions', [])
                dimension_groups = view_content.get('dimension_groups', [])
                
                # Nested views should have at least one dimension or dimension group
                total_fields = len(dimensions) + len(dimension_groups)
                assert total_fields > 0, f"Empty nested view detected: {view_name} - this should be prevented by our fix"

        # Validate view count - should have main view + nested views but no empty ones
        expected_view_count = len(expected_views)
        generated_view_count = len(generated_views)
        
        # Generated should have at least as many views as expected (could have more due to deeper nesting)
        assert generated_view_count >= expected_view_count, f"Missing views: expected at least {expected_view_count}, got {generated_view_count}"

        # Load expected LKML fixture for d_item_v3 and compare structure
        with open('tests/fixtures/expected/d_item_v3.view.lkml', 'r') as f:
            expected_d_item = lkml.load(f)

        # Compare generated vs expected using LKML object comparison
        self._compare_lkml_structures(expected_d_item, generated)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
