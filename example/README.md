# Examples

In order to genarate looker views from dbt models, you can use the `dbt2lookml` command to generate from the test fixtures 

The examples in this directory uses the test fixtures which can be found in the following locations

* /tests/fixtures/catalog.json
* /tests/fixtures/manifest.json

Same ones is used for some of the tests

### Example of dbt model -> looker view conversion

```shell
poetry run dbt2lookml --target-dir tests/fixtures --output-dir example/default
```

The output you can see under example/default/ folder

### Example of dbt model -> looker view conversion with using the table names instead of the dbt model names

```shell
poetry run dbt2lookml --target-dir tests/fixtures --output-dir example/use-table-name
```

The output you can see under example/use-table-name/ folder

### Example of dbt model -> looker view conversion but skipping the explore joins

```shell
poetry run dbt2lookml --target-dir tests/fixtures --output-dir example/skip-explore
```

The output you can see under examples/skip-explore/ folder


