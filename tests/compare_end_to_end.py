#!/usr/bin/env python3
"""
Script to compare expected vs generated LKML for end-to-end tests
"""

import lkml
import json
from pathlib import Path

def compare_views():
    """Compare expected vs generated LKML views"""
    
    # Load expected and generated files
    expected_path = Path('tests/fixtures/expected/dq_icasoi_current.view.lkml')
    generated_path = Path('output/tests/conlaybi/item_dataquality/conlaybi_item_dataquality__dq_icasoi_current.view.lkml')
    
    if not expected_path.exists():
        print(f"Expected file not found: {expected_path}")
        return
        
    if not generated_path.exists():
        print(f"Generated file not found: {generated_path}")
        return
    
    with open(expected_path, 'r') as f:
        expected = lkml.load(f)

    with open(generated_path, 'r') as f:
        generated = lkml.load(f)

    # Get main views
    expected_main = next((v for v in expected['views'] if v['name'] == 'dq_icasoi_current'), None)
    generated_main = next((v for v in generated['views'] if v['name'] == 'conlaybi_item_dataquality__dq_icasoi_current'), None)
    
    if not expected_main:
        print("Expected main view 'dq_icasoi_current' not found")
        return
        
    if not generated_main:
        print("Generated main view 'conlaybi_item_dataquality__dq_icasoi_current' not found")
        return

    print('=== MAIN VIEW COMPARISON ===')
    print(f'Expected view name: {expected_main["name"]}')
    print(f'Generated view name: {generated_main["name"]}')
    print()

    print('=== SQL TABLE NAME ===')
    print(f'Expected: {expected_main.get("sql_table_name", "N/A")}')
    print(f'Generated: {generated_main.get("sql_table_name", "N/A")}')
    print()

    # Compare dimensions
    expected_dims = expected_main.get('dimensions', [])
    generated_dims = generated_main.get('dimensions', [])
    
    print('=== DIMENSION COUNT ===')
    print(f'Expected dimensions: {len(expected_dims)}')
    print(f'Generated dimensions: {len(generated_dims)}')
    print()

    # Get dimension names
    expected_dim_names = {dim['name'] for dim in expected_dims}
    generated_dim_names = {dim['name'] for dim in generated_dims}
    
    print('=== DIMENSION DIFFERENCES ===')
    missing_from_generated = expected_dim_names - generated_dim_names
    extra_in_generated = generated_dim_names - expected_dim_names
    
    if missing_from_generated:
        print(f'Missing from generated ({len(missing_from_generated)}):')
        for dim in sorted(missing_from_generated):
            print(f'  - {dim}')
    else:
        print('No dimensions missing from generated')
    
    print()
    if extra_in_generated:
        print(f'Extra in generated ({len(extra_in_generated)}):')
        for dim in sorted(extra_in_generated):
            # Check if it's hidden
            dim_obj = next((d for d in generated_dims if d['name'] == dim), None)
            hidden = ' [HIDDEN]' if dim_obj and dim_obj.get('hidden') == 'yes' else ''
            print(f'  - {dim}{hidden}')
    else:
        print('No extra dimensions in generated')
    
    print()
    
    # Compare dimension groups
    expected_dgs = expected_main.get('dimension_groups', [])
    generated_dgs = generated_main.get('dimension_groups', [])
    
    print('=== DIMENSION GROUPS ===')
    print(f'Expected dimension groups: {len(expected_dgs)}')
    print(f'Generated dimension groups: {len(generated_dgs)}')
    
    if expected_dgs:
        print('Expected:')
        for dg in expected_dgs:
            print(f'  - {dg["name"]} ({dg.get("type", "unknown")})')
    
    if generated_dgs:
        print('Generated:')
        for dg in generated_dgs:
            print(f'  - {dg["name"]} ({dg.get("type", "unknown")})')
    
    print()
    
    # Show sample dimensions from each
    print('=== SAMPLE EXPECTED DIMENSIONS ===')
    for i, dim in enumerate(expected_dims[:5]):
        print(f'{i+1}. {dim["name"]} ({dim.get("type", "unknown")})')
        if 'sql' in dim:
            print(f'   SQL: {dim["sql"]}')
    
    print()
    print('=== SAMPLE GENERATED DIMENSIONS ===')
    for i, dim in enumerate(generated_dims[:5]):
        hidden = ' [HIDDEN]' if dim.get('hidden') == 'yes' else ''
        print(f'{i+1}. {dim["name"]} ({dim.get("type", "unknown")}){hidden}')
        if 'sql' in dim:
            print(f'   SQL: {dim["sql"]}')
    
    print()
    print('=== VIEW COUNT COMPARISON ===')
    print(f'Expected views: {len(expected["views"])}')
    print(f'Generated views: {len(generated["views"])}')
    
    print('\nExpected view names:')
    for view in expected['views']:
        print(f'  - {view["name"]}')
    
    print('\nGenerated view names:')
    for view in generated['views']:
        print(f'  - {view["name"]}')

if __name__ == '__main__':
    compare_views()
