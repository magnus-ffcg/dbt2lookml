# Contributing to dbt2lookml

Thank you for your interest in contributing to dbt2lookml! This guide will help you get started.

## Development Setup

See the [Development Documentation](https://magnus-ffcg.github.io/dbt2lookml/development/contributing/) for detailed setup instructions using UV.

## Quick Start for Contributors

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/dbt2lookml.git
   cd dbt2lookml
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Run tests to ensure everything works**
   ```bash
   uv run pytest
   ```

## Making Changes

### Code Style

We use automated formatting and linting:

```bash
# Format code
uv run black dbt2lookml tests
uv run isort dbt2lookml tests

# Check linting
uv run flake8 dbt2lookml tests --max-line-length=130

# Type checking
uv run mypy dbt2lookml
```

### Testing

- **Write tests** for new features and bug fixes
- **Run the full test suite** before submitting:
  ```bash
  uv run pytest tests/ -v --cov=dbt2lookml
  ```
- **Test coverage** should not decrease

### Commit Guidelines

Follow conventional commit format:
```
type: short description

feat: add new dimension type support
fix: resolve nested array SQL generation
docs: update installation instructions
test: add coverage for edge cases
refactor: simplify dimension naming logic
```

## Pull Request Process

1. **Create a feature branch** from `main`
2. **Make your changes** with tests
3. **Ensure all tests pass** and code is formatted
4. **Update documentation** if needed
5. **Submit a pull request** with:
   - Clear description of changes
   - Reference to any related issues
   - Screenshots/examples if applicable

## Code Review

- All PRs require review before merging
- Address feedback promptly
- Keep PRs focused and reasonably sized
- Rebase on main if needed

## Reporting Issues

When reporting bugs or requesting features:

- Use the GitHub issue templates
- Provide minimal reproduction steps
- Include relevant environment details
- Check for existing issues first

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

## Questions?

- Check the [documentation](https://magnus-ffcg.github.io/dbt2lookml/)
- Open a GitHub issue for bugs/features
- Start a GitHub discussion for questions

We appreciate all contributions, from bug reports to code improvements!