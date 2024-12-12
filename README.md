# dbt2lookml
Use `dbt2lookml` to generate Looker view files automatically from dbt models in Bigquery.

This is a fork of forks of dbt2looker and dbt2looker-biqquery and took a similar but not identical approach and this sort went in the direction of a new package called dbt2lookml. Should pretty much work the same as dbt2looker-bigquery.

It has been tested with dbt v1.8 and generated 2800+ views in roughly 6 seconds.

## Installation

### Through pip:

```shell
pip install dbt2lookml
```
### Through poetry:

```shell
git clone https://github.com/magnus-ffcg/dbt2lookml.git
cd dbt2lookml
poetry install
```

## Quickstart

Run `dbt2lookml` in the root of your dbt project *after compiling dbt docs*.
(dbt2lookml uses docs to infer types and such)

**If you are using poetry:**
You need to append "poetry run" in beginning of each command

```shell
poetry run dbt2lookml [args e.g. --target-dir [dbt-repo]/target --output-dir output]
```

**When running for the first time make sure dbt has the data available:**
```shell
dbt docs generate
```
**Generate Looker view files for all models:**
```shell
dbt2lookml --target-dir [dbt-repo]/target --output-dir output
```

**Generate Looker view files for all models tagged `prod`**
```shell
dbt2lookml [default args] --tag prod
```

**Generate Looker view files for dbt named `test`**
```shell
dbt2lookml [default args] --select test
```

**Generate Looker view files for all exposed models**
[dbt docs - exposures](https://docs.getdbt.com/docs/build/exposures)
```shell
dbt2lookml [default args] --exposures-only
```

**Generate Looker view files for all exposed models and specific tags**
```shell
dbt2lookml [default args] --exposures-only --exposures-tag looker
```

**Generate Looker view files but skip the explore and its joins**
```shell
dbt2lookml [default args] --skip-explore
```

**Generate Looker view files but use table name as view name**
```shell
dbt2lookml [default args]--use-table-name
```

**Generate Looker view files but also generate a locale file**
```shell
dbt2lookml [default args]--generate-locale
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