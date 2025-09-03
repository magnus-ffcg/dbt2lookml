# Quick Start

This guide will help you generate your first LookML views from dbt models in just a few minutes.

## Prerequisites

1. **dbt project** with BigQuery models
2. **dbt2lookml installed** (see [Installation](installation.md))
3. **dbt docs generated** (required for manifest and catalog files)

## Step 1: Generate dbt Documentation

First, generate the dbt documentation files that dbt2lookml needs:

```bash
cd your-dbt-project
dbt docs generate
```

This creates:
- `target/manifest.json` - Model definitions and metadata
- `target/catalog.json` - Column information from BigQuery

## Step 2: Basic Generation

Generate LookML views for all your models:

```bash
dbt2lookml --target-dir target --output-dir output
```

**Parameters:**
- `--target-dir target` - Directory containing manifest.json and catalog.json
- `--output-dir output` - Where to save generated LookML files

## Step 3: Review Generated Files

Check the `output` directory for your generated LookML files:

```
output/
├── schema1/
│   ├── model1.view.lkml
│   └── model2.view.lkml
└── schema2/
    └── model3.view.lkml
```

## Example Output

Here's what a generated view looks like:

```lookml
view: customer_orders {
  sql_table_name: `project.dataset.customer_orders` ;;

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: order_date {
    type: date
    datatype: date
    sql: ${TABLE}.order_date ;;
  }

  dimension: total_amount {
    type: number
    sql: ${TABLE}.total_amount ;;
  }

  measure: count {
    type: count
  }
}
```

## Common Use Cases

### Filter by Tags

Generate views only for models tagged with 'prod':

```bash
dbt2lookml --target-dir target --output-dir output --tag prod
```

### Specific Models

Generate views for specific models:

```bash
dbt2lookml --target-dir target --output-dir output --select customer_orders
```

### Validate Generated LookML

Validate generated LookML files for syntax errors:

```bash
# Validate generated LookML files
dbt2lookml --target-dir target --output-dir output --validate
```

### Configuration File

For complex setups, use a configuration file:

```yaml
# config.yaml
target_dir: "./target"
output_dir: "./lookml"
tag: "analytics"
continue_on_error: true
```

```bash
dbt2lookml --config config.yaml
```

## Next Steps

- [Configuration](configuration.md) - Learn about advanced configuration options
- [Basic Usage](../user-guide/basic-usage.md) - Explore more features
- [Looker Metadata](../user-guide/looker-metadata.md) - Customize your LookML output
