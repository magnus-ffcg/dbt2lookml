# generated by datamodel-codegen:
#   filename:  catalog.v1.json
#   timestamp: 2024-12-07T11:29:14+00:00

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Extra, Field


class Metadata(BaseModel):
    class Config:
        extra = Extra.forbid

    dbt_schema_version: Optional[str] = None
    dbt_version: Optional[str] = '1.7.14'
    generated_at: Optional[str] = None
    invocation_id: Optional[str] = None
    env: Optional[Dict[str, str]] = None


class Metadata1(BaseModel):
    class Config:
        extra = Extra.forbid

    type: str
    schema_: str = Field(..., alias='schema')
    name: str
    database: Optional[str] = None
    comment: Optional[str] = None
    owner: Optional[str] = None


class Columns(BaseModel):
    class Config:
        extra = Extra.forbid

    type: str
    index: int
    name: str
    comment: Optional[str] = None


class Stats(BaseModel):
    class Config:
        extra = Extra.forbid

    id: str
    label: str
    value: Optional[Union[bool, str, float]]
    include: bool
    description: Optional[str] = None


class Nodes(BaseModel):
    class Config:
        extra = Extra.forbid

    metadata: Metadata1 = Field(..., title='TableMetadata')
    columns: Dict[str, Columns]
    stats: Dict[str, Stats]
    unique_id: Optional[str] = None


class Sources(Nodes):
    pass


class CatalogArtifact(BaseModel):
    class Config:
        extra = Extra.forbid

    metadata: Metadata = Field(..., title='CatalogMetadata')
    nodes: Dict[str, Nodes]
    sources: Dict[str, Sources]
    errors: Optional[List[str]] = None
    field_compile_results: Any = Field(None, alias='_compile_results')