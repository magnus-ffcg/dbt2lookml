# dbt2lookml
Use `dbt2lookml` to generate Looker view files automatically from dbt models in Bigquery.

This is a fork of forks of dbt2looker and dbt2looker-biqquery and took a similar but not identical approach and this sort went in the direction of a new package called dbt2lookml. Should pretty much work the same as dbt2looker-bigquery.

It has been tested with dbt v1.8 and generated 2800+ views in roughly 6 seconds.

## Why do you need dbt2lookml?

For very few data-models and/or small analytics team, lookers built in lookml generation works fine
and is likely preferrred. For larger teams and/or many/complex models, lookers are a bit of a pain to build manually or semi-automatically through looker.

So dbt2lookml is not built to be a replacement for looker, but instead a way to generate looker views from dbt models in a more automated way.

## How is it supposed to work?

As an example, in our project we build dbt through google workflows, after dbt run, we generate dbt docs and elementary. In the end of the workflow, we trigger a pub/sub message that dbt has finished.
Based on this message, a cloud function is triggered, which then runs dbt2lookml to generate views
In the same cloud function, we also publish the changes to a specific git repository called lookml-base
In our main looker project we import the views from the lookml-base repository and then we handle the extends, explores, etc in there. A recommendation is to use dbt versioning to stabilize the lookml files, so you can have multiple versions of the same dbt model and therefor control which base-view you should use in your extended view. This way we can have constant updates of the views when they change instead having to be reactive to changes in the dbt models.

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