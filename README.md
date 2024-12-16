# dbt2lookml

[![PyPI version](https://badge.fury.io/py/dbt2lookml.svg)](https://badge.fury.io/py/dbt2lookml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generate Looker view files automatically from dbt models in BigQuery. Tested with dbt v1.8, capable of generating 2800+ views in roughly 6 seconds.

## Overview

`dbt2lookml` bridges the gap between dbt and Looker by automatically generating LookML views from your dbt models. It's particularly valuable for:

- Large data teams managing numerous models
- Complex data models with many relationships
- Teams wanting to automate their Looker view generation
- Projects requiring consistent view definitions across dbt and Looker

## Features

- 🚀 Fast generation of LookML views from dbt models
- 🔄 Automatic type mapping from BigQuery to Looker
- 🏷️ Support for dbt tags and exposures
- 🔗 Automatic join detection and generation
- 📝 Custom measure definition support
- 🌐 Locale file generation support

## Installation

### Via pip
```bash
pip install dbt2lookml
```

### Via poetry
```bash
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
poetry install
```

## Quick Start

1. **Generate dbt docs** (required for getting a manifest and catalog file to generate views from):
   ```bash
   dbt docs generate
   ```

2. **Generate Looker views**:
   ```bash
   dbt2lookml --target-dir target --output-dir output
   ```

## Usage Examples

### Filter by Tags
```bash
# Generate views for models tagged 'prod'
dbt2lookml --target-dir target --output-dir output --tag prod
```

### Filter by Model Name
```bash
# Generate view for model named 'test'
dbt2lookml --target-dir target --output-dir output --select test
```

### Work with Exposures
```bash
# Generate views for exposed models only
dbt2lookml --target-dir target --output-dir output --exposures-only

# Generate views for exposed models with specific tag
dbt2lookml --target-dir target --output-dir output --exposures-only --exposures-tag looker
```

### Additional Options
```bash
# Skip explore generation
dbt2lookml --target-dir target --output-dir output --skip-explore

# Use table names instead of model names
dbt2lookml --target-dir target --output-dir output --use-table-name

# Generate locale file
dbt2lookml --target-dir target --output-dir output --generate-locale
```

## Integration Example

Here's how you might integrate dbt2lookml in a production workflow:

1. Run dbt through Google Cloud Workflows
2. Generate dbt docs and elementary
3. Trigger a Pub/Sub message on completion
4. Cloud Function runs dbt2lookml
5. Push generated views to lookml-base repository
6. Import views in main Looker project

## Configuration

### Defining Looker Metadata

Add Looker-specific configuration in your dbt `schema.yml`:

```yaml
models:
  - name: model-name
    columns:
      - name: url
        description: "Page URL"
        meta:
          looker:
            dimension:
              hidden: true
              label: "Page URL"
              group_label: "Page Info"
            measures:
              - type: count_distinct
                sql_distinct_key: ${url}
                label: "Unique Pages"
              - type: count
                value_format_name: decimal_1
                label: "Total Page Views"
    meta:
      looker:
        joins:
          - join: users
            sql_on: "${users.id} = ${model-name.user_id}"
            type: left_outer
            relationship: many_to_one
```

#### Supported Metadata Options

##### Dimension Options
- `hidden`: Boolean
- `label`: String
- `group_label`: String
- `value_format_name`: String

##### Measure Options
- `type`: String (count, sum, average, etc.)
- `sql_distinct_key`: String
- `value_format_name`: String
- `filters`: Array of filter objects

##### Join Options
- `sql_on`: String (join condition)
- `type`: String (left_outer, inner, etc.)
- `relationship`: String (many_to_one, one_to_many, etc.)

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Higly inspired by [dbt-looker](https://github.com/looker/dbt-looker) and [dbt2looker-bigquery](https://github.com/looker/dbt2looker-bigquery).
