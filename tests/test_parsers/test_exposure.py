"""Tests for the exposure parser module."""
import pytest
from dbt2lookml.models.dbt import DbtManifest
from dbt2lookml.parsers.exposure import ExposureParser

class TestExposureParser:
    @pytest.fixture
    def sample_manifest(self):
        return DbtManifest(**{
            "metadata": {"adapter_type": "bigquery"},
            "nodes": {},
            "exposures": {
                "exposure.test.dashboard1": {
                    "resource_type": "exposure",
                    "name": "dashboard1",
                    "type": "dashboard",
                    "tags": ["analytics_dashboard"],
                    "depends_on": ["model.test.model1"],
                    "unique_id": "exposure.test.dashboard1",
                    "refs": [{"name": "model1", "package": "test"}],
                }
            }
        })

    @pytest.fixture
    def parser(self, sample_manifest):
        return ExposureParser(sample_manifest)

    def test_get_exposures(self, parser):
        """Test getting exposed model names."""
        # Test without tag filter
        exposed_names = parser.get_exposures()
        assert len(exposed_names) == 1
        assert exposed_names == ["model1"]

        # Test with matching tag
        exposed_names = parser.get_exposures(exposures_tag="analytics_dashboard")
        assert len(exposed_names) == 1
        assert exposed_names == ["model1"]

        # Test with non-matching tag
        exposed_names = parser.get_exposures(exposures_tag="non_existent_tag")
        assert len(exposed_names) == 0
