# Basic Usage

This guide covers the fundamental features of dbt2lookml and how to use them effectively.

## Core Workflow

The typical dbt2lookml workflow follows these steps:

1. **Prepare dbt project** - Ensure your dbt models are ready
2. **Generate dbt docs** - Create manifest and catalog files
3. **Run dbt2lookml** - Generate LookML views
4. **Review output** - Check generated files
5. **Import to Looker** - Use the generated LookML in your Looker project

## Basic Commands

### Generate All Views

```bash
dbt2lookml --target-dir target --output-dir output
```

This generates LookML views for all models in your dbt project.

### Filter by Model Name

```bash
# Single model
dbt2lookml --target-dir target --output-dir output --select customer_orders

# Multiple models (space-separated)
dbt2lookml --target-dir target --output-dir output --select customer_orders product_sales
```

### Filter by Tags

```bash
# Models tagged with 'analytics'
dbt2lookml --target-dir target --output-dir output --tag analytics

# Models tagged with 'prod'
dbt2lookml --target-dir target --output-dir output --tag prod
```

## Understanding Output Structure

dbt2lookml organizes output files by schema:

```
output/
├── analytics/
│   ├── customer_orders.view.lkml
│   ├── product_sales.view.lkml
│   └── customer_orders__items.view.lkml  # Nested view
├── marketing/
│   ├── campaigns.view.lkml
│   └── email_metrics.view.lkml
└── finance/
    └── revenue_summary.view.lkml
```

### File Naming Conventions

- **Main views**: `{model_name}.view.lkml`
- **Nested views**: `{model_name}__{nested_field}.view.lkml`
- **Schema directories**: Match BigQuery dataset names

## Generated LookML Structure

### Basic View

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

  measure: count {
    type: count
  }
}
```

### Dimension Groups (Date/Time)

```lookml
dimension_group: created {
  type: time
  timeframes: [raw, date, week, month, quarter, year]
  convert_tz: no
  datatype: date
  sql: ${TABLE}.created_date ;;
}
```

### Nested Structures

For BigQuery ARRAY and STRUCT types:

```lookml
# Main view
view: orders {
  dimension: order_id {
    type: string
    sql: ${TABLE}.order_id ;;
  }
  
  # Hidden dimension for nested array
  dimension: items {
    hidden: yes
    sql: ${TABLE}.items ;;
  }
}

# Nested view
view: orders__items {
  dimension: product_id {
    type: string
    sql: ${TABLE}.product_id ;;
  }
  
  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }
}
```

## Type Mapping

dbt2lookml automatically maps BigQuery types to Looker types:

| BigQuery Type | Looker Type | Notes |
|---------------|-------------|-------|
| STRING | string | - |
| INTEGER, INT64 | number | - |
| FLOAT, FLOAT64 | number | - |
| BOOLEAN, BOOL | yesno | - |
| DATE | date | Creates dimension_group |
| DATETIME | datetime | Creates dimension_group |
| TIMESTAMP | datetime | Creates dimension_group |
| TIME | string | - |
| ARRAY | - | Creates nested view |
| STRUCT | - | Flattened or nested view |

## Common Patterns

### Include/Exclude Models

```bash
# Include specific models
dbt2lookml --target-dir target --output-dir output \
  --include-models customer_orders product_sales

# Exclude test models
dbt2lookml --target-dir target --output-dir output \
  --exclude-models test_* staging_*
```

### Error Handling

```bash
# Continue processing even if some models fail
dbt2lookml --target-dir target --output-dir output --continue-on-error
```

### Verbose Output

```bash
# Debug mode for troubleshooting
dbt2lookml --target-dir target --output-dir output --log-level DEBUG
```

## Best Practices

### 1. Use Tags for Organization

Tag your dbt models to control which views are generated:

```yaml
# dbt_project.yml
models:
  my_project:
    analytics:
      +tags: ["analytics", "prod"]
    staging:
      +tags: ["staging"]
```

### 2. Regular Regeneration

Set up automated regeneration when dbt models change:

```bash
#!/bin/bash
# regenerate-lookml.sh
dbt docs generate
dbt2lookml --config prod.config.yaml
```

### 3. Review Generated Files

Always review generated LookML before deploying:

- Check dimension names and types
- Verify nested structure handling
- Ensure proper SQL references

### 4. Version Control

Include generated LookML in version control to track changes:

```gitignore
# Don't ignore generated LookML
!output/
```

## Troubleshooting

### Common Issues

**Missing manifest.json or catalog.json**
```bash
# Solution: Generate dbt docs first
dbt docs generate
```

**No models found**
```bash
# Check your filters
dbt2lookml --target-dir target --output-dir output --log-level DEBUG
```

**Permission errors**
```bash
# Ensure output directory is writable
mkdir -p output
chmod 755 output
```

## Next Steps

- [Advanced Features](advanced-features.md) - Explore advanced capabilities
- [Model Filtering](model-filtering.md) - Learn advanced filtering techniques
- [Looker Metadata](looker-metadata.md) - Customize your LookML output
