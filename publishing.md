# How to publish a new version to PyPI

## Manual Publishing with UV

```bash
# Update version in pyproject.toml manually or with your preferred method
# Then build and publish in one command:
uv publish
```

Or if you want to build first and then publish:

```bash
uv build
uv publish
```

## Automated Publishing via GitHub Actions

The preferred method is to use the automated GitHub Actions workflow:

1. Update the version in `pyproject.toml`
2. Commit and push changes
3. Create and push a version tag:
   ```bash
   git tag v0.2.6  # Replace with your version
   git push origin v0.2.6
   ```
4. The GitHub Actions workflow will automatically:
   - Validate the tag matches the package version
   - Run tests
   - Build the package
   - Publish to PyPI using trusted publishing
   - Create a GitHub release

## Requirements

- For manual publishing: PyPI API token configured in `~/.pypirc`
- For automated publishing: Trusted publishing configured on PyPI for this repository