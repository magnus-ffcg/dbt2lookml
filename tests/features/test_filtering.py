"""Test include/exclude models functionality using existing test fixtures."""

import json
import os

from dbt2lookml.parsers.base import DbtParser


def test_include_exclude_with_fixtures():
    """Test include/exclude models using existing test fixtures."""

    # Load existing fixtures
    fixtures_dir = os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'data')
    with open(os.path.join(fixtures_dir, 'manifest.json')) as f:
        manifest = json.load(f)
    with open(os.path.join(fixtures_dir, 'catalog.json')) as f:
        catalog = json.load(f)

    # Create mock args
    class MockArgs:
        def __init__(self, include_models=None, exclude_models=None):
            self.include_models = include_models
            self.exclude_models = exclude_models
            self.select = None
            self.tag = None
            self.exposures_only = False
            self.exposures_tag = None

# Test 1: No filtering - should return all models
    args1 = MockArgs()
    parser1 = DbtParser(args1, manifest, catalog)
    all_models = parser1.get_models()
    # Test 2: Include specific model
    args2 = MockArgs(include_models=['conlaybi_item_dataquality__dq_ICASOI_Current'])
    parser2 = DbtParser(args2, manifest, catalog)
    included_models = parser2.get_models()
    # Test 3: Exclude specific model (should return empty)
    args3 = MockArgs(exclude_models=['conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day'])
    parser1 = DbtParser(args1, manifest, catalog)
    all_models = parser1.get_models()
    parser2 = DbtParser(args2, manifest, catalog)
    included_models = parser2.get_models()
    parser3 = DbtParser(args3, manifest, catalog)
    excluded_models = parser3.get_models()
    parser4 = DbtParser(MockArgs(include_models=['nonexistent_model']), manifest, catalog)
    nonexistent_models = parser4.get_models()
    # Assertions
    assert len(all_models) == 3, "Should have exactly three models"
    assert len(included_models) == 1, "Should have exactly one included model"
    assert included_models[0].name == 'conlaybi_item_dataquality__dq_ICASOI_Current'
    assert len(excluded_models) == 2, "Should have two models after excluding one"
    assert len(nonexistent_models) == 0, "Should have no models when including nonexistent"
