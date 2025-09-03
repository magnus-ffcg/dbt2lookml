# dbt2lookml

[![PyPI version](https://badge.fury.io/py/dbt2lookml.svg)](https://badge.fury.io/py/dbt2lookml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-github--pages-blue)](https://magnus-ffcg.github.io/dbt2lookml/)

Generate Looker view files automatically from dbt models in BigQuery. Tested with dbt v1.8, capable of generating 2800+ views in roughly 6 seconds.

## Quick Start

📖 **[Full Documentation & Usage Guide](https://magnus-ffcg.github.io/dbt2lookml/)**

For usage instructions, configuration, and examples, see the comprehensive documentation.

## Development

For development setup, testing, and contribution guidelines, see the **[Development Documentation](https://magnus-ffcg.github.io/dbt2lookml/development/contributing/)**.

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
