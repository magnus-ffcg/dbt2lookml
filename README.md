# dbt2lookml
Use `dbt2lookml` to generate Looker view files automatically from dbt models in Bigquery.

This is a fork of forks of dbt2looker and dbt2looker-biqquery and took a similar but not identical approach and this sort went in the direction of a new-package dbt2lookml. Should pretty much work the same as dbt2looker-bigquery.

It has been tested with dbt v1.8 and generated 2800+ views in roughly 6 seconds.

## Installation

NOTE: this is not pypi package yet, it will come soon.

```shell
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
poetry install
```

## Quickstart

Run `dbt2lookml` in the root of your dbt project *after compiling dbt docs*.
(dbt2lookml uses docs to infer types and such)

**Generate Looker view files for all models:**
```shell
dbt docs generate
(poetry run) dbt2lookml
```

**Generate Looker view files for all models tagged `prod`**
```shell
(poetry run) dbt2lookml --tag prod
```

**Generate Looker view files for dbt named `test`**
```shell
(poetry run) dbt2lookml --select test
```

**Generate Looker view files for all exposed models**
[dbt docs - exposures](https://docs.getdbt.com/docs/build/exposures)
```shell
(poetry run) dbt2lookml --exposures-only
```

**Generate Looker view files for all exposed models and specific tags**
```shell
(poetry run) dbt2lookml --exposures-only --exposures-tag looker
```

**Generate Looker view files but skip the explore and its joins**
```shell
(poetry run) dbt2lookml --skip-explore
```

**Generate Looker view files but use table name as view name**
```shell
(poetry run) dbt2lookml --use-table-name
```

**Generate Looker view files but also generate a locale file**
```shell
(poetry run) dbt2lookml --generate-locale
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
              dimension:
                hidden: True
                label: event
                group_label: identifiers
                value_format_name: id
              measures:
                - type: count_distinct
                  sql_distinct_key: ${url}
                - type: count
                  value_format_name: decimal_1
    meta:
      looker:
        joins:
          - join: users
            sql_on: "${users.id} = ${model-name.user_id}"
            type: left_outer
            relationship: many_to_one