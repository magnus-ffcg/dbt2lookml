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
- ⚙️ YAML configuration file support
- 🎯 Advanced model filtering (include/exclude lists)
- 📅 Customizable timeframes for date/time dimensions
- 🏛️ ISO week and year fields support
- 🔧 Conflict resolution for dimension names
- 🏗️ Nested structure support for complex data types
- 📊 Enhanced measure types and precision settings
- 🛡️ Error handling with continue-on-error mode

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

### Basic Usage
```bash
# Generate all views
dbt2lookml --target-dir target --output-dir output

# Using configuration file
dbt2lookml --config config.yaml
```

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

### Advanced Model Filtering
```bash
# Include specific models
dbt2lookml --target-dir target --output-dir output --include-models customer_model order_model

# Exclude specific models
dbt2lookml --target-dir target --output-dir output --exclude-models test_model dev_model

# Combine filters
dbt2lookml --target-dir target --output-dir output --tag analytics --exclude-models legacy_*
```

### Work with Exposures
```bash
# Generate views for exposed models only
dbt2lookml --target-dir target --output-dir output --exposures-only

# Generate views for exposed models with specific tag
dbt2lookml --target-dir target --output-dir output --exposures-only --exposures-tag looker
```

### Date/Time Configuration
```bash
# Include ISO week and year fields
dbt2lookml --target-dir target --output-dir output --include-iso-fields

# Skip explore generation for nested structures
dbt2lookml --target-dir target --output-dir output --skip-explore

# Use table names instead of model names
dbt2lookml --target-dir target --output-dir output --use-table-name

# Generate locale files
dbt2lookml --target-dir target --output-dir output --generate-locale

# Continue on error
dbt2lookml --target-dir target --output-dir output --continue-on-error
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

### YAML Configuration File

Use a YAML configuration file to manage complex settings:

```yaml
# config.yaml
target_dir: "./target"
output_dir: "./lookml"
log_level: "INFO"

# Model filtering
tag: "analytics"
include_models:
  - "customer_model"
  - "order_model"
exclude_models:
  - "test_model"
  - "dev_model"

# LookML generation options
use_table_name: false
skip_explore: false
generate_locale: false
continue_on_error: true

# Custom timeframes
timeframes:
  date:
    - "raw"
    - "date"
    - "week"
    - "month"
    - "quarter"
    - "year"
  time:
    - "raw"
    - "time"
    - "date"
    - "week"
    - "month"
    - "year"

# Advanced options
remove_schema_string: "my_project_"
exposures_only: false
exposures_tag: "production"
```

Use with: `dbt2lookml --config config.yaml`

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
              value_format_name: decimal_0
              convert_tz: true
              can_filter: true
            measures:
              - type: count_distinct
                sql_distinct_key: ${url}
                label: "Unique Pages"
                approximate: true
                approximate_threshold: 1000
              - type: count
                value_format_name: decimal_1
                label: "Total Page Views"
                filters:
                  - filter_dimension: status
                    filter_expression: "completed"
      - name: revenue
        meta:
          looker:
            measures:
              - type: sum
                precision: 2
                label: "Total Revenue"
              - type: average
                precision: 2
                label: "Average Revenue"
    meta:
      looker:
        label: "Page Analytics"
        description: "Page view analytics with user data"
        joins:
          - join: users
            sql_on: "${users.id} = ${model-name.user_id}"
            type: left_outer
            relationship: many_to_one
```

#### Supported Metadata Options

##### Dimension Options
- `hidden`: Boolean - Hide dimension from field picker
- `label`: String - Custom label for the dimension
- `group_label`: String - Group dimensions under this label
- `value_format_name`: String - Format for displaying values
- `convert_tz`: Boolean - Convert timezone for datetime fields
- `can_filter`: Boolean/String - Control filtering capabilities
- `timeframes`: Array - Custom timeframes for dimension groups

##### Measure Options
- `type`: String - Measure type (count, sum, average, count_distinct, etc.)
- `sql_distinct_key`: String - Key for distinct counts
- `value_format_name`: String - Format for displaying values
- `filters`: Array - Filter objects with `filter_dimension` and `filter_expression`
- `approximate`: Boolean - Use approximate algorithms for count_distinct
- `approximate_threshold`: Integer - Threshold for approximate algorithms
- `precision`: Integer - Decimal precision for average/sum measures
- `percentile`: Integer - Percentile value for percentile measures

##### Join Options
- `sql_on`: String - SQL condition for the join
- `type`: String - Join type (left_outer, inner, right_outer, full_outer)
- `relationship`: String - Relationship type (many_to_one, one_to_many, one_to_one)

##### View Options
- `hidden`: Boolean - Hide entire view from field picker
- `label`: String - Custom label for the view
- `description`: String - Description for the view

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Higly inspired by [dbt-looker](https://github.com/looker/dbt-looker) and [dbt2looker-bigquery](https://github.com/looker/dbt2looker-bigquery).
