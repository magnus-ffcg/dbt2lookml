# Contributing

We welcome contributions to dbt2lookml! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- Poetry (recommended) or pip
- Git

### Clone and Setup

```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
poetry install
```

Or with pip:
```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
pip install -e ".[test]"
```

### Run Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=dbt2lookml

# Run specific test file
poetry run pytest tests/unit/generators/test_dimension.py
```

## Code Style

We use several tools to maintain code quality:

### Black (Code Formatting)
```bash
poetry run black dbt2lookml tests
```

### isort (Import Sorting)
```bash
poetry run isort dbt2lookml tests
```

### Flake8 (Linting)
```bash
poetry run flake8 dbt2lookml tests
```

### Type Checking
```bash
poetry run mypy dbt2lookml
```

## Project Structure

```
dbt2lookml/
├── dbt2lookml/           # Main package
│   ├── generators/       # LookML generators
│   ├── models/          # Data models
│   ├── parsers/         # dbt file parsers
│   └── cli.py           # Command-line interface
├── tests/               # Test suite
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   └── fixtures/       # Test fixtures
└── docs/               # Documentation
```

## Contributing Guidelines

### 1. Fork and Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write clean, readable code
- Follow existing patterns and conventions
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run tests
poetry run pytest

# Check code style
poetry run black --check dbt2lookml tests
poetry run isort --check-only dbt2lookml tests
poetry run flake8 dbt2lookml tests
```

### 4. Commit Changes

Use clear, descriptive commit messages:

```bash
git commit -m "feat: add support for custom dimension types"
git commit -m "fix: resolve nested array SQL generation issue"
git commit -m "docs: update configuration examples"
```

### 5. Submit Pull Request

- Create a pull request against the `main` branch
- Include a clear description of changes
- Reference any related issues
- Ensure all tests pass

## Types of Contributions

### Bug Reports

When reporting bugs, please include:

- Python version
- dbt2lookml version
- Steps to reproduce
- Expected vs actual behavior
- Sample dbt models (if applicable)

### Feature Requests

For new features, please:

- Describe the use case
- Explain the expected behavior
- Consider backward compatibility
- Provide examples if possible

### Code Contributions

We welcome:

- Bug fixes
- New features
- Performance improvements
- Documentation updates
- Test coverage improvements

## Development Guidelines

### Writing Tests

- Write unit tests for new functions/methods
- Add integration tests for end-to-end functionality
- Use descriptive test names
- Mock external dependencies

Example test:
```python
def test_dimension_conflict_resolution():
    """Test that conflicting dimensions are properly renamed."""
    generator = LookmlDimensionGenerator(mock_args)
    
    dimensions = [{"name": "created_date", "type": "date"}]
    dimension_groups = [{"name": "created", "type": "time"}]
    
    result = generator.resolve_conflicts(dimensions, dimension_groups)
    
    assert result[0]["name"] == "created_date_conflict"
    assert "hidden: yes" in result[0]["attributes"]
```

### Documentation

- Update docstrings for new functions
- Add examples to user guides
- Update API reference as needed
- Keep changelog current

### Performance Considerations

- Profile code for performance bottlenecks
- Use efficient algorithms and data structures
- Consider memory usage for large projects
- Test with realistic data sizes

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release PR
4. Tag release after merge
5. Publish to PyPI

## Getting Help

- Check existing [issues](https://github.com/magnus-ffcg/dbt2lookml/issues)
- Start a [discussion](https://github.com/magnus-ffcg/dbt2lookml/discussions)
- Review the [documentation](https://magnus-ffcg.github.io/dbt2lookml/)

## Code of Conduct

Please be respectful and constructive in all interactions. We're building this tool together to help the data community.

## Recognition

Contributors will be recognized in:
- Release notes
- README acknowledgments
- Project documentation

Thank you for contributing to dbt2lookml! 🎉
