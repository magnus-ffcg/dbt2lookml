"""Test LookML Measure Generator implementations."""

from argparse import Namespace

import pytest

from dbt2lookml.enums import LookerMeasureType, LookerValueFormatName
from dbt2lookml.generators.measure import LookmlMeasureGenerator
from dbt2lookml.models.dbt import DbtModel, DbtModelColumn, DbtModelColumnMeta, DbtModelMeta, DbtResourceType
from dbt2lookml.models.looker import DbtMetaLooker, DbtMetaLookerMeasure, DbtMetaLookerMeasureFilter


@pytest.fixture
def cli_args():
    """Create CLI args fixture."""
    return Namespace(
        use_table_name=False,
        include_explore=False,
        include_models=[],
        exclude_models=[],
        target_dir='output',
    )


def test_lookml_measures_from_model(cli_args):
    """Test measure generation from model."""
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
                    )
                ),
            ),
        },
        meta=DbtModelMeta(),
        unique_id="test_model",
        resource_type=DbtResourceType.MODEL,
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
    """Test measure generation with filters."""
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
        resource_type=DbtResourceType.MODEL,
        schema="test_schema",
        description="Test model",
        tags=[],
    )
    measures = measure_generator.lookml_measures_from_model(model)
    assert len(measures) == 1
    measure = measures[0]
    assert measure["type"] == LookerMeasureType.SUM.value
    assert measure["filters"] == [{"field": "status", "value": "completed"}]
