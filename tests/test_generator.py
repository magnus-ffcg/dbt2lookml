"""Test LookML Generator implementations."""
import pytest
from dbt2lookml.generators import LookmlGenerator
from dbt2lookml.generators.dimension import LookmlDimensionGenerator
from dbt2lookml.generators.measure import LookmlMeasureGenerator
from dbt2lookml.generators.utils import map_bigquery_to_looker
from dbt2lookml.utils import Sql
from dbt2lookml.models.dbt import (
    DbtModel,
    DbtModelMeta,
    DbtModelColumn,
    DbtModelColumnMeta,
    DbtCatalogNodeColumn,
)
from dbt2lookml.models.looker import (
    DbtMetaLooker,
    DbtMetaLookerBase,
    DbtMetaLookerDimension,
    DbtMetaLookerMeasure,
    DbtMetaLookerMeasureFilter,
    DbtMetaLookerJoin,
)
from dbt2lookml.enums import (
    SupportedDbtAdapters,
    LookerMeasureType,
    LookerTimeTimeframes,
    LookerDateTypes,
    LookerDateTimeTypes,
    LookerDateTimeframes,
    LookerValueFormatName,
)
from dbt2lookml.exceptions import CliError, NotImplementedError

import os
import tempfile
import json
from argparse import Namespace


@pytest.fixture
def cli_args():
    """Fixture for CLI arguments."""
    return Namespace(
        use_table_name=False,
        build_explore=False,
        table_format_sql=True,
    )


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("${TABLE}.column", "${TABLE}.column"),
        ("${view_name}.field", "${view_name}.field"),
        ("invalid sql", None),  # No ${} syntax
        ("SELECT * FROM table;;", None),  # Invalid ending
        ("${TABLE}.field;;", "${TABLE}.field"),  # Removes semicolons
    ],
)
def test_validate_sql(sql, expected):
    """Test SQL validation for Looker expressions"""
    sql_util = Sql()
    assert sql_util.validate_sql(sql) == expected


@pytest.mark.parametrize(
    "bigquery_type,expected_looker_type",
    [
        ("STRING", "string"),
        ("INT64", "number"),
        ("FLOAT64", "number"),
        ("BOOL", "yesno"),
        ("TIMESTAMP", "timestamp"),
        ("DATE", "date"),
        ("DATETIME", "datetime"),
        ("ARRAY<STRING>", "string"),
        ("STRUCT<name STRING>", "string"),
        ("INVALID_TYPE", None),
    ],
)
def test_map_bigquery_to_looker(bigquery_type, expected_looker_type):
    """Test mapping of BigQuery types to Looker types"""
    assert map_bigquery_to_looker(bigquery_type) == expected_looker_type


def test_dimension_group_time(cli_args):
    """Test creation of time-based dimension groups"""
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={},
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type="model",
        schema="test_schema",
        description="Test model",
        tags=[],
    )

    column = DbtModelColumn(
        name="created_at",
        lookml_name="created_at",
        lookml_long_name="created_at",
        data_type="TIMESTAMP",
        unique_id="test_model.created_at",
        meta=DbtModelColumnMeta(),
    )

    dimension_generator = LookmlDimensionGenerator(cli_args)
    result = dimension_generator.lookml_dimension_group(column, "time", True, model)
    assert isinstance(result[0], dict)
    assert result[0].get("type") == "time"
    assert result[0].get("timeframes") == LookerTimeTimeframes.values()
    assert result[0].get("convert_tz") == "yes"


def test_dimension_group_date(cli_args):
    """Test creation of date-based dimension groups"""
    dimension_generator = LookmlDimensionGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={},
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type="model",
        schema="test_schema",
        description="Test model",
        tags=[],
    )

    # Test with date column
    column = DbtModelColumn(
        name="created_date",
        lookml_name="created_date",
        lookml_long_name="created_date",
        data_type="DATE",
        unique_id="test_model.created_date",
        meta=DbtModelColumnMeta(
            looker=DbtMetaLooker(
                dimension=DbtMetaLookerDimension(
                    label="Custom Date Label", 
                    group_label="Custom Group"
                )
            )
        ),
    )

    dimension_group, dimension_set, _ = dimension_generator.lookml_dimension_group(
        column, "date", True, model
    )
    assert dimension_group['type'] == 'date'
    assert dimension_group['convert_tz'] == 'no'
    assert dimension_group['timeframes'] == LookerDateTimeframes.values()
    assert dimension_group['label'] == "Custom Date Label"
    assert dimension_group['group_label'] == "Custom Group"
    assert dimension_group['name'] == "created"  # _date removed

    # Check dimension set
    assert dimension_set['name'] == "s_created"
    assert all(
        tf in dimension_set['fields']
        for tf in [f"created_{t}" for t in LookerDateTimeframes.values()]
    )


def test_lookml_dimensions_with_metadata(cli_args):
    """Test dimension generation with various metadata options"""
    dimension_generator = LookmlDimensionGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={
            "string_col": DbtModelColumn(
                name="string_col",
                lookml_name="string_col",
                lookml_long_name="string_col",
                data_type="STRING",
                unique_id="test_model.string_col",
                description="Custom Description",
                meta=DbtModelColumnMeta(
                    looker=DbtMetaLooker(
                        dimension=DbtMetaLookerDimension(
                            label="Custom Label",
                            group_label="Custom Group",
                            value_format_name=LookerValueFormatName.USD,
                            description="Custom Description",
                        )
                    ),
                ),
            )
        },
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type="model",
        schema="test_schema",
        description="Test model",
        tags=[],
    )

    dimensions, _ = dimension_generator.lookml_dimensions_from_model(model)
    assert len(dimensions) == 1
    dimension = dimensions[0]
    assert dimension["name"] == "string_col"
    assert dimension["label"] == "Custom Label"
    assert dimension["group_label"] == "Custom Group"
    assert dimension["description"] == "Custom Description"
    assert dimension["value_format_name"] == LookerValueFormatName.USD.value


def test_lookml_measures_from_model(cli_args):
    """Test measure generation from model"""
    measure_generator = LookmlMeasureGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={
            "amount": DbtModelColumn(
                name="amount",
                lookml_name="amount",
                lookml_long_name="amount",
                data_type="FLOAT64",
                unique_id="test_model.amount",
                meta=DbtModelColumnMeta(
                    looker=DbtMetaLooker(
                        measures=[
                            DbtMetaLookerMeasure(
                                type=LookerMeasureType.SUM,
                                label="Total Amount",
                                description="Sum of all amounts",
                                value_format_name=LookerValueFormatName.USD,
                            )
                        ]
                    ),
                ),
            ),

        },
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type="model",
        schema="test_schema",
        description="Test model",
        tags=[],
    )

    measures = measure_generator.lookml_measures_from_model(model)
    assert len(measures) == 1
    measure = measures[0]
    assert measure["type"] == LookerMeasureType.SUM.value
    assert measure["label"] == "Total Amount"
    assert measure["description"] == "Sum of all amounts"
    assert measure["value_format_name"] == LookerValueFormatName.USD.value
    assert measure["sql"] == "${TABLE}.amount"


def test_lookml_measures_with_filters(cli_args):
    """Test measure generation with filters"""
    measure_generator = LookmlMeasureGenerator(cli_args)
    model = DbtModel(
        name="test_model",
        path="models/test_model.sql",
        relation_name="`project.dataset.table_name`",
        columns={
            "amount": DbtModelColumn(
                name="amount",
                lookml_name="amount",
                lookml_long_name="amount",
                data_type="FLOAT64",
                unique_id="test_model.amount",
                meta=DbtModelColumnMeta(
                    looker=DbtMetaLooker(
                        measures=[
                            DbtMetaLookerMeasure(
                                type=LookerMeasureType.SUM,
                                filters=[
                                    DbtMetaLookerMeasureFilter(
                                        filter_dimension="status",
                                        filter_expression="completed",
                                    )
                                ],
                            )
                        ]
                    )
                ),
            )
        },
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type="model",
        schema="test_schema",
        description="Test model",
        tags=[],
    )

    measures = measure_generator.lookml_measures_from_model(model)
    assert len(measures) == 1
    measure = measures[0]
    assert measure["type"] == LookerMeasureType.SUM.value
    assert measure["filters"] == [{"field": "status", "value": "completed"}]
