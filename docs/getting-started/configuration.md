# Configuration

dbt2lookml supports flexible configuration through command-line arguments and YAML configuration files.

## Command Line Arguments

### Basic Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--target-dir` | Directory containing manifest.json and catalog.json | `target` |
| `--output-dir` | Output directory for generated LookML files | `output` |
| `--config` | Path to YAML configuration file | - |
| `--log-level` | Logging level (DEBUG, INFO, WARN, ERROR) | `INFO` |

### Model Filtering

| Argument | Description | Example |
|----------|-------------|---------|
| `--tag` | Filter models by dbt tag | `--tag analytics` |
| `--select` | Select specific model by name | `--select customer_orders` |
| `--include-models` | Include specific models (space-separated) | `--include-models model1 model2` |
| `--exclude-models` | Exclude specific models (space-separated) | `--exclude-models test_* dev_*` |

### Generation Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--use-table-name` | Use table names instead of model names | `false` |
| `--generate-locale` | Generate locale files | `false` |
| `--include-iso-fields` | Include ISO week and year fields | `false` |
| `--validate` | Validate generated LookML files for syntax errors | `false` |
| `--continue-on-error` | Continue processing on errors | `false` |

### Exposure Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--exposures-only` | Generate views for exposed models only | `false` |
| `--exposures-tag` | Filter exposures by tag | - |

## YAML Configuration File

For complex configurations, use a YAML file:

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
generate_locale: false
continue_on_error: true
include_iso_fields: true
validate: false

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

## Environment-Specific Configurations

### Development
```yaml
# dev.config.yaml
target_dir: "./target"
output_dir: "./dev-lookml"
tag: "dev"
continue_on_error: true
log_level: "DEBUG"
```

### Production
```yaml
# prod.config.yaml
target_dir: "./target"
output_dir: "./prod-lookml"
tag: "prod"
exposures_only: true
continue_on_error: false
log_level: "INFO"
```

## Configuration Precedence

Configuration values are applied in this order (later values override earlier ones):

1. Default values
2. YAML configuration file
3. Command-line arguments

## Advanced Configuration

### Custom Timeframes

Define custom timeframes for date and time dimensions:

```yaml
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
    - "hour"
    - "date"
    - "week"
    - "month"
    - "year"
```

### Schema String Removal

Remove prefixes from schema names:

```yaml
remove_schema_string: "dwh_"
```

This transforms `dwh_analytics` → `analytics` in output paths.

## Validation

dbt2lookml validates configuration on startup:

- Checks that required directories exist
- Validates manifest.json and catalog.json files
- Warns about invalid configuration options

## Next Steps

- [Basic Usage](../user-guide/basic-usage.md) - Learn core features
- [Advanced Features](../user-guide/advanced-features.md) - Explore advanced options
- [Looker Metadata](../user-guide/looker-metadata.md) - Customize LookML output
