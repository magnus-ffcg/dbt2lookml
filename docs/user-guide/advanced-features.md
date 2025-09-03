# Advanced Features

This guide covers the advanced capabilities of dbt2lookml for power users and complex use cases.

## Nested Structure Handling

dbt2lookml excels at handling complex BigQuery nested structures like ARRAY and STRUCT types.

### ARRAY Types

BigQuery ARRAY columns are converted to nested views:

```sql
-- BigQuery table
CREATE TABLE orders (
  order_id STRING,
  items ARRAY<STRUCT<
    product_id STRING,
    quantity INT64,
    price FLOAT64
  >>
)
```

Generated LookML:
```lookml
# orders.view.lkml
view: orders {
  dimension: order_id {
    type: string
    sql: ${TABLE}.order_id ;;
  }
  
  dimension: items {
    hidden: yes
    sql: ${TABLE}.items ;;
  }
}

# orders__items.view.lkml  
view: orders__items {
  dimension: product_id {
    type: string
    sql: ${TABLE}.product_id ;;
  }
  
  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }
  
  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
  }
}
```

### STRUCT Types

STRUCT columns are flattened into the main view:

```sql
-- BigQuery table
CREATE TABLE customers (
  customer_id STRING,
  address STRUCT<
    street STRING,
    city STRING,
    country STRING
  >
)
```

Generated LookML:
```lookml
view: customers {
  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }
  
  dimension: address_street {
    type: string
    sql: ${TABLE}.address.street ;;
  }
  
  dimension: address_city {
    type: string
    sql: ${TABLE}.address.city ;;
  }
  
  dimension: address_country {
    type: string
    sql: ${TABLE}.address.country ;;
  }
}
```

## Dimension Conflict Resolution

When dimension names conflict (e.g., between regular dimensions and dimension groups), dbt2lookml automatically resolves conflicts:

```lookml
# Original conflicting dimensions
dimension: created_date {
  type: date
  sql: ${TABLE}.created_date ;;
}

dimension_group: created {
  type: time
  timeframes: [date, week, month]
  sql: ${TABLE}.created_timestamp ;;
}

# After conflict resolution
dimension: created_date_conflict {
  type: date
  sql: ${TABLE}.created_date ;;
  hidden: yes # Renamed from 'created_date' due to conflict with dimension group 'created'
}

dimension_group: created {
  type: time
  timeframes: [date, week, month]
  sql: ${TABLE}.created_timestamp ;;
}
```

## Custom Timeframes

Configure custom timeframes for date and time dimensions:

```yaml
# config.yaml
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
    - "quarter"
    - "year"
```

## ISO Week and Year Fields

Enable ISO week and year fields for better international date handling:

```bash
dbt2lookml --target-dir target --output-dir output --include-iso-fields
```

Generated output:
```lookml
dimension_group: created {
  type: time
  timeframes: [raw, date, week, month, quarter, year, week_of_year, iso_week_of_year]
  convert_tz: no
  datatype: date
  sql: ${TABLE}.created_date ;;
}

dimension: created_iso_week_of_year {
  type: number
  sql: EXTRACT(ISOWEEK FROM ${TABLE}.created_date) ;;
}

dimension: created_iso_year {
  type: number
  sql: EXTRACT(ISOYEAR FROM ${TABLE}.created_date) ;;
}
```

## Explore Generation

Generate explore definitions within view files for nested structures:

```bash
dbt2lookml --target-dir target --output-dir output --include-explore
```

Generated output:
```lookml
view: orders {
  # ... dimensions ...
}

view: orders__items {
  # ... dimensions ...
}

explore: orders {
  join: orders__items {
    view_label: "Items"
    sql: LEFT JOIN UNNEST(${orders.items}) as orders__items ;;
    relationship: one_to_many
  }
}
```

## Locale File Generation

Generate locale files for internationalization:

```bash
dbt2lookml --target-dir target --output-dir output --generate-locale
```

Creates locale files:
```yaml
# en.yml
en:
  views:
    orders:
      label: "Orders"
      description: "Customer order data"
      dimensions:
        order_id:
          label: "Order ID"
          description: "Unique order identifier"
```

## Advanced Model Filtering

### Pattern Matching

Use wildcards for flexible model selection:

```bash
# Include all models starting with 'fact_'
dbt2lookml --target-dir target --output-dir output --include-models "fact_*"

# Exclude all test models
dbt2lookml --target-dir target --output-dir output --exclude-models "*_test" "test_*"
```

### Multiple Tags

Filter by multiple tags:

```bash
# Models tagged with 'analytics'
dbt2lookml --target-dir target --output-dir output --tag analytics
```

### Exposure-Based Generation

Generate views only for models used in exposures:

```bash
# All exposed models
dbt2lookml --target-dir target --output-dir output --exposures-only

# Exposed models with specific tag
dbt2lookml --target-dir target --output-dir output --exposures-only --exposures-tag looker
```

## Error Handling and Debugging

### Continue on Error

Process all models even if some fail:

```bash
dbt2lookml --target-dir target --output-dir output --continue-on-error
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
dbt2lookml --target-dir target --output-dir output --log-level DEBUG
```

Debug output includes:
- Model processing details
- Type mapping decisions
- Conflict resolution steps
- SQL generation logic

## Performance Optimization

### Concurrent Processing

dbt2lookml processes models concurrently for better performance:

```python
# Automatically uses available CPU cores
# Processes 2800+ views in ~6 seconds
```

### Memory Management

For large projects, consider:

```bash
# Process in smaller batches
dbt2lookml --target-dir target --output-dir output --tag batch1
dbt2lookml --target-dir target --output-dir output --tag batch2
```

## Integration Patterns

### CI/CD Pipeline

```yaml
# .github/workflows/generate-lookml.yml
name: Generate LookML
on:
  push:
    paths: ['models/**']

jobs:
  generate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dbt2lookml
        run: pip install dbt2lookml
      - name: Generate docs
        run: dbt docs generate
      - name: Generate LookML
        run: dbt2lookml --config prod.config.yaml
      - name: Commit changes
        run: |
          git add output/
          git commit -m "Update generated LookML"
          git push
```

### Cloud Function Integration

```python
# Google Cloud Function
import subprocess
from google.cloud import storage

def generate_lookml(event, context):
    # Download dbt artifacts
    # Run dbt2lookml
    subprocess.run(['dbt2lookml', '--config', 'prod.config.yaml'])
    # Upload to Looker repository
```

## Advanced Configuration

### Schema String Removal

Clean up schema names in output paths:

```yaml
# config.yaml
remove_schema_string: "dwh_"
```

Transforms:
- `dwh_analytics` → `analytics`
- `dwh_marketing` → `marketing`

### Custom Output Structure

Control output directory structure:

```yaml
# config.yaml
use_table_name: true  # Use BigQuery table names instead of dbt model names
```

## Next Steps

- [Model Filtering](model-filtering.md) - Master advanced filtering techniques
- [Looker Metadata](looker-metadata.md) - Customize LookML with dbt metadata
- [API Reference](../api/cli.md) - Detailed API documentation
