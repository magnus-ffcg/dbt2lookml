# Installation

## Requirements

- Python 3.9 or higher
- dbt Core 1.0 or higher
- Access to BigQuery

## Installation Methods

### Via pip (Recommended)

Install the latest stable version from PyPI:

```bash
pip install dbt2lookml
```

### Via UV (Development)

For development or if you prefer UV:

```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
uv sync --all-extras
```

### From Source

To install the latest development version:

```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
pip install -e .
```

## Verify Installation

Check that the installation was successful:

```bash
dbt2lookml --version
```

You should see the version number displayed.

## Dependencies

The following packages are automatically installed:

- `lkml>=1.1` - LookML parsing and generation
- `pydantic>=2.9` - Data validation
- `PyYAML>=5.0` - YAML configuration support
- `rich>=13.9.4` - Enhanced terminal output
- `unidecode>=1.3.0` - Unicode handling

## Next Steps

Once installed, proceed to the [Quick Start](quick-start.md) guide to generate your first LookML views.
