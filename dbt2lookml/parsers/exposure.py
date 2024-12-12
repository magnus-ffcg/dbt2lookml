"""Exposure-related parsing functionality."""

from typing import List, Optional

from dbt2lookml.models.dbt import DbtManifest, DbtExposure


class ExposureParser:
    """Parser for DBT exposures."""

    def __init__(self, manifest: DbtManifest):
        """Initialize with manifest data."""
        self._manifest = manifest

    def get_exposures(
        self, exposures_tag: Optional[str] = None, model_name: Optional[str] = None
    ) -> List[str]:
        """Get list of exposed model names.

        Args:
            exposures_tag: Optional tag to filter exposures by
            model_name: Optional model name to filter exposures by

        Returns:
            List of exposure names that match the filter criteria
        """
        exposures = [
            exp
            for exp in self._manifest.exposures.values()
            if isinstance(exp, DbtExposure) and exp.resource_type == 'exposure'
        ]

        if exposures_tag:
            exposures = [
                exp for exp in exposures if exp.tags is not None and exposures_tag in exp.tags
            ]

        if model_name:
            exposures = [exp for exp in exposures if model_name in exp.depends_on.nodes]

        return list({ref.name for exposure in exposures for ref in exposure.refs})
