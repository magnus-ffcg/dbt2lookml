from typing import Union, Dict, List, Optional
from pydantic import BaseModel, ValidationInfo, Field, validator, field_validator, model_validator
from dbt2lookml.enums import (
    LookerMeasureType,
    LookerValueFormatName,
    LookerTimeFrame,
    LookerJoinType,
    LookerRelationshipType,
)


class LookViewFile(BaseModel):
    '''A file in a looker view directory'''

    filename: str
    contents: str
    db_schema: str = Field(..., alias='schema')


class DbtMetaLookerBase(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: Optional[bool] = None


class DbtMetaLookerDimension(DbtMetaLookerBase):
    '''Looker-specific metadata for a dimension on a dbt model column

    meta:
        looker:
            dimension:
                hidden: True
                label: "Blog Info"
                group_label: "Blog Info"
                description: "Blog Info"
                value_format_name: decimal_0
                filters:
                  - path: "/blog%"

    '''

    convert_tz: Optional[bool] = Field(default=None)
    group_label: Optional[str] = Field(default=None)
    value_format_name: Optional[LookerValueFormatName] = Field(default=None)
    timeframes: Optional[List[LookerTimeFrame]] = Field(default=None)
    can_filter: Optional[Union[bool, str]] = Field(default=None)


class DbtMetaLookerMeasureFilter(BaseModel):
    filter_dimension: str
    filter_expression: str


class DbtMetaLookerMeasure(DbtMetaLookerBase):
    '''Looker metadata for a measure.'''

    # Required fields
    type: LookerMeasureType

    # Common optional fields
    name: Optional[str] = None
    group_label: Optional[str] = None
    value_format_name: Optional[LookerValueFormatName] = None
    filters: Optional[List[DbtMetaLookerMeasureFilter]] = None

    # Fields specific to certain measure types
    approximate: Optional[bool] = None  # For count_distinct
    approximate_threshold: Optional[int] = None  # For count_distinct
    precision: Optional[int] = None  # For average, sum
    sql_distinct_key: Optional[str] = None  # For count_distinct
    percentile: Optional[int] = None  # For percentile measures

    @model_validator(mode='after')
    def validate_measure_attributes(self) -> 'DbtMetaLookerMeasure':
        """Validate that measure attributes are compatible with the measure type."""
        measure_type = self.type

        # Validate type-specific attributes
        if (
            any(
                v is not None
                for v in [self.approximate, self.approximate_threshold, self.sql_distinct_key]
            )
            and measure_type != LookerMeasureType.COUNT_DISTINCT
        ):
            raise ValueError(
                "approximate, approximate_threshold, and sql_distinct_key can only be used with count_distinct measures"
            )

        if self.percentile is not None and not measure_type.value.startswith('percentile'):
            raise ValueError("percentile can only be used with percentile measures")

        if self.precision is not None and measure_type not in [
            LookerMeasureType.AVERAGE,
            LookerMeasureType.SUM,
        ]:
            raise ValueError("precision can only be used with average or sum measures")

        return self


class DbtMetaLookerJoin(BaseModel):
    '''Looker-specific metadata for joins on a dbt model

    meta:
      looker:
        description: "Page views for Hubble landing page"
        label: "Page Views"
        joins:
          - join: users                               # Reference another dbt model to join to
            sql_on: "${users.id} = ${pages.user_id}"  # Sql string containing join clause
            type: left_outer                          # Optional - left_outer is default
            relationship: many_to_one                 # Relationship type
    '''

    join_model: Optional[str] = Field(default=None)
    sql_on: Optional[str] = Field(default=None)
    type: Optional[LookerJoinType] = Field(default=None)
    relationship: Optional[LookerRelationshipType] = Field(default=None)


class DbtMetaLooker(BaseModel):
    """Looker metadata for a model."""

    # Component fields
    view: Optional[DbtMetaLookerBase] = None
    dimension: Optional[DbtMetaLookerDimension] = None
    measures: Optional[List[DbtMetaLookerMeasure]] = Field(default=[])
    joins: Optional[List[DbtMetaLookerJoin]] = Field(default=[])
