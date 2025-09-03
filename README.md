# dbt2lookml

[![PyPI version](https://badge.fury.io/py/dbt2lookml.svg)](https://badge.fury.io/py/dbt2lookml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-github--pages-blue)](https://magnus-ffcg.github.io/dbt2lookml/)

Generate Looker view files automatically from dbt models in BigQuery. Tested with dbt v1.8, capable of generating 2800+ views in roughly 6 seconds.

## Quick Start

📖 **[Full Documentation & Usage Guide](https://magnus-ffcg.github.io/dbt2lookml/)**

For usage instructions, configuration, and examples, see the comprehensive documentation.

## Development Setup

### Prerequisites

- Python 3.9+
- Poetry (recommended for development)

### Clone and Install

```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
poetry install
```

### Run Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=dbt2lookml

# Run specific test
poetry run pytest tests/unit/generators/test_dimension.py
```

### Code Quality

```bash
# Format code
poetry run black dbt2lookml tests

# Sort imports
poetry run isort dbt2lookml tests

# Lint code
poetry run flake8 dbt2lookml tests

# Type checking
poetry run mypy dbt2lookml
```

### Documentation

```bash
# Serve docs locally
poetry run mkdocs serve

# Build docs
poetry run mkdocs build
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
└── docs/               # Documentation source
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](https://magnus-ffcg.github.io/dbt2lookml/development/contributing/) for details on our development workflow and coding standards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Higly inspired by [dbt-looker](https://github.com/looker/dbt-looker) and [dbt2looker-bigquery](https://github.com/looker/dbt2looker-bigquery).
