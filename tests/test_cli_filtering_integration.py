"""Integration tests for CLI include/exclude model filtering."""

import json
import os
import tempfile
from unittest.mock import Mock

from dbt2lookml.cli import Cli


class TestCliFilteringIntegration:
    """Integration tests for CLI model filtering."""

    def create_test_manifest(self):
        """Create a minimal test manifest with multiple models."""
        return {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "dbt_version": "1.7.0",
                "generated_at": "2024-01-01T00:00:00Z",
                "invocation_id": "test-invocation-id",
                "env": {},
                "project_name": "test_project",
                "project_id": "test_project_id",
                "user_id": "test_user_id",
                "send_anonymous_usage_stats": False,
                "adapter_type": "bigquery",
            },
            "nodes": {
                "model.test.model1": {
                    "resource_type": "model",
                    "name": "model1",
                    "schema": "test_schema",
                    "database": "test_db",
                    "relation_name": "test_db.test_schema.model1",
                    "unique_id": "model.test.model1",
                    "tags": ["tag1"],
                    "description": "Test model 1",
                    "columns": {
                        "id": {"name": "id", "data_type": "INTEGER"},
                        "name": {"name": "name", "data_type": "STRING"},
                    },
                    "path": "models/model1.sql",
                    "meta": {},
                },
                "model.test.model2": {
                    "resource_type": "model",
                    "name": "model2",
                    "schema": "test_schema",
                    "database": "test_db",
                    "relation_name": "test_db.test_schema.model2",
                    "unique_id": "model.test.model2",
                    "tags": ["tag2"],
                    "description": "Test model 2",
                    "columns": {
                        "id": {"name": "id", "data_type": "INTEGER"},
                        "email": {"name": "email", "data_type": "STRING"},
                    },
                    "path": "models/model2.sql",
                    "meta": {},
                },
                "model.test.model3": {
                    "resource_type": "model",
                    "name": "model3",
                    "schema": "test_schema",
                    "database": "test_db",
                    "relation_name": "test_db.test_schema.model3",
                    "unique_id": "model.test.model3",
                    "tags": ["tag3"],
                    "description": "Test model 3",
                    "columns": {
                        "id": {"name": "id", "data_type": "INTEGER"},
                        "status": {"name": "status", "data_type": "STRING"},
                    },
                    "path": "models/model3.sql",
                    "meta": {},
                },
            },
            "sources": {},
            "exposures": {},
            "metrics": {},
        }

    def create_test_catalog(self):
        """Create a minimal test catalog."""
        return {
            "nodes": {
                "model.test.model1": {
                    "metadata": {
                        "type": "VIEW",
                        "schema": "test_schema",
                        "name": "model1",
                        "database": "test_db",
                    },
                    "columns": {
                        "id": {"type": "INTEGER", "name": "id", "index": 1},
                        "name": {"type": "STRING", "name": "name", "index": 2},
                    },
                },
                "model.test.model2": {
                    "metadata": {
                        "type": "VIEW",
                        "schema": "test_schema",
                        "name": "model2",
                        "database": "test_db",
                    },
                    "columns": {
                        "id": {"type": "INTEGER", "name": "id", "index": 1},
                        "email": {"type": "STRING", "name": "email", "index": 2},
                    },
                },
                "model.test.model3": {
                    "metadata": {
                        "type": "VIEW",
                        "schema": "test_schema",
                        "name": "model3",
                        "database": "test_db",
                    },
                    "columns": {
                        "id": {"type": "INTEGER", "name": "id", "index": 1},
                        "status": {"type": "STRING", "name": "status", "index": 2},
                    },
                },
            }
        }

    def test_cli_include_models_single(self):
        """Test CLI with single include model."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = ['model2']
            args.exclude_models = None
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert only model2 is returned
            assert len(models) == 1
            assert models[0].name == 'model2'

    def test_cli_include_models_multiple(self):
        """Test CLI with multiple include models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = ['model1', 'model3']
            args.exclude_models = None
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert correct models are returned
            assert len(models) == 2
            model_names = [m.name for m in models]
            assert 'model1' in model_names
            assert 'model3' in model_names
            assert 'model2' not in model_names

    def test_cli_exclude_models_single(self):
        """Test CLI with single exclude model."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = None
            args.exclude_models = ['model2']
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert model2 is excluded
            assert len(models) == 2
            model_names = [m.name for m in models]
            assert 'model1' in model_names
            assert 'model3' in model_names
            assert 'model2' not in model_names

    def test_cli_exclude_models_multiple(self):
        """Test CLI with multiple exclude models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = None
            args.exclude_models = ['model1', 'model3']
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert only model2 remains
            assert len(models) == 1
            assert models[0].name == 'model2'

    def test_cli_include_exclude_combined(self):
        """Test CLI with both include and exclude models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = ['model1', 'model2', 'model3']
            args.exclude_models = ['model2']
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert model2 is excluded from included models
            assert len(models) == 2
            model_names = [m.name for m in models]
            assert 'model1' in model_names
            assert 'model3' in model_names
            assert 'model2' not in model_names

    def test_cli_select_overrides_include(self):
        """Test that select argument takes precedence over include models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            # Mock CLI arguments
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = 'model2'
            args.tag = None
            args.include_models = ['model1', 'model3']
            args.exclude_models = None
            args.exposures_only = False
            args.exposures_tag = None
            # Parse models
            models = cli.parse(args)
            # Assert select takes precedence
            assert len(models) == 1
            assert models[0].name == 'model2'

    def test_cli_config_file_include_exclude(self):
        """Test that config file include/exclude values are used when CLI args are None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            manifest_path = os.path.join(tmpdir, 'manifest.json')
            catalog_path = os.path.join(tmpdir, 'catalog.json')
            config_path = os.path.join(tmpdir, 'config.yaml')
            with open(manifest_path, 'w') as f:
                json.dump(self.create_test_manifest(), f)
            with open(catalog_path, 'w') as f:
                json.dump(self.create_test_catalog(), f)
            with open(config_path, 'w') as f:
                f.write(
                    """
include_models:
  - model1
  - model3
exclude_models:
  - model2
"""
                )
            # Mock CLI arguments with None values
            cli = Cli()
            args = Mock()
            args.manifest_path = manifest_path
            args.catalog_path = catalog_path
            args.target_dir = tmpdir
            args.select = None
            args.tag = None
            args.include_models = None  # Should use config value
            args.exclude_models = None  # Should use config value
            args.exposures_only = False
            args.exposures_tag = None
            args.config = config_path
            # Load and merge config with args (simulating what run() does)
            config = cli._load_config(config_path)
            merged_args = cli._merge_config_with_args(args, config)
            # Parse models with merged args
            models = cli.parse(merged_args)
            # Assert config values are used
            assert len(models) == 2
            model_names = [m.name for m in models]
            assert 'model1' in model_names
            assert 'model3' in model_names
            assert 'model2' not in model_names
