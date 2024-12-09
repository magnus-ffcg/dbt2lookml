# How to publish a new version to Pypi
poetry version && poetry build && twine upload dist/* --repository dbt2lookml