# dbt2lookml
Use `dbt2lookml` to generate Looker view files automatically from dbt models in Bigquery.

This is a fork of forks of dbt2looker and dbt2lookml took a similar but not identical approach.

## Quickstart

Run `dbt2lookml` in the root of your dbt project *after compiling dbt docs*.
(dbt2lookml uses docs to infer types and such)

**Generate Looker view files for all models:**
```shell
dbt docs generate
dbt2lookml
```

**Generate Looker view files for all models tagged `prod`**
```shell
dbt2lookml --tag prod
```

**Generate Looker view files for dbt named `test`**
```shell
dbt2lookml --select test
```

**Generate Looker view files for all exposed models **
[dbt docs - exposures](https://docs.getdbt.com/docs/build/exposures)
```shell
dbt2lookml --exposures-only
```

**Generate Looker view files for all exposed models and specific tags**
```shell
dbt2lookml --exposures-only --exposures-tag looker
```

**Generate Looker view files but skip the explore and its joins**
```shell
dbt2lookml --skip-explore-joins
```

**Generate Looker view files but use table name as view name **
```shell
dbt2lookml --use-table-name
```

**Generate Looker view files but also generate a locale file **
```shell
dbt2lookml --generate-locale
```

## Defining measures or other metadata for looker

You can define looker measures in your dbt `schema.yml` files. For example:

```yaml
models:
  - name: model-name
    columns:
      - name: url
        description: "Page url"
      - name: event_id
        description: unique event id for page view
        meta:
            looker:
              hidden: True
              label: event
              group_label: identifiers
              value_format_name: id
              
            looker_measures:
              - type: count_distinct
                sql_distinct_key: ${url}
              - type: count
                value_format_name: decimal_1

```
