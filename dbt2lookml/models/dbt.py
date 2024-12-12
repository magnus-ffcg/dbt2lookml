import logging
from typing import Union, Dict, List, Optional
from pydantic import field_validator, model_validator, BaseModel, Field
from dbt2lookml import enums
from dbt2lookml.schema import SchemaParser

schema_parser = SchemaParser()

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from dbt2lookml.exceptions import UnsupportedDbtAdapterError
from dbt2lookml.models.looker import DbtMetaLooker


def yes_no_validator(value: Union[bool, str]):
    '''Convert booleans or strings to lookml yes/no syntax'''
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    elif value.lower() in ['yes', 'no']:
        return value.lower()
    elif value.lower() in ['true', 'false']:
        return 'yes' if value.lower() == 'true' else 'no'
    else:
        logging.warning(f'Value must be "yes", "no", or a boolean. Got {value}')
        return None


class DbtNode(BaseModel):
    '''A dbt node. extensible to models, seeds, etc.'''

    name: str
    unique_id: str
    resource_type: Literal[
        'model',
        'seed',
        'snapshot',
        'analysis',
        'test',
        'operation',
        'sql_operation',
        'source',
        'macro',
        'exposure',
        'metric',
        'group',
        'saved_query',
        'semantic_model',
        'unit_test',
        'fixture',
    ]


class DbtExposureRef(BaseModel):
    '''A reference in a dbt exposure'''

    name: str
    package: Optional[str] = None
    version: Optional[Union[str, int]] = None


class DbtDependsOn(BaseModel):
    """A model for dependencies between dbt objects.

    Contains lists of macros and nodes that an object depends on.
    """

    macros: List[str] = []
    nodes: List[str] = []


class DbtExposure(DbtNode):
    '''A dbt exposure'''

    name: str
    description: Optional[str] = None
    url: Optional[str] = None
    refs: List[DbtExposureRef]
    tags: Optional[List[str]] = []  # Adds exposure tags
    depends_on: DbtDependsOn = DbtDependsOn()


class DbtCatalogNodeMetadata(BaseModel):
    '''Metadata about a dbt catalog node'''

    type: str
    db_schema: str = Field(..., alias='schema')
    name: str
    comment: Optional[str] = None
    owner: Optional[str] = None


class DbtCatalogNodeColumn(BaseModel):
    '''A column in a dbt catalog node'''

    type: str
    data_type: Optional[str] = 'MISSING'
    inner_types: Optional[List[str]] = []
    comment: Optional[str] = None
    index: int
    name: str
    # child_name: Optional[str]
    # parent: Optional[str]  # Added field to store the parent node
    parent: Optional['DbtCatalogNodeColumn'] = None

    @model_validator(mode="before")
    @classmethod
    def validate_inner_type(cls, values):
        column_type = values.get('type')

        def truncate_before_character(string, character):
            # Find the position of the character in the string.
            pos = string.find(character)

            # If found, return everything up to that point.
            return string[:pos] if pos != -1 else string

        data_type = truncate_before_character(column_type, '<')
        values['data_type'] = truncate_before_character(data_type, '(')

        inner_types = schema_parser.parse(column_type)
        if inner_types and inner_types != column_type:
            values['inner_types'] = inner_types

        return values


class DbtCatalogNodeRelationship(BaseModel):
    '''A model for nodes containing relationships'''

    type: str
    columns: List[DbtCatalogNodeColumn]
    relationships: List[str]  # List of relationships, adjust the type accordingly


class DbtCatalogNode(BaseModel):
    '''A dbt catalog node'''

    metadata: DbtCatalogNodeMetadata
    columns: Dict[str, DbtCatalogNodeColumn]

    @field_validator('columns')
    @classmethod
    def case_insensitive_column_names(cls, v: Dict[str, DbtCatalogNodeColumn]):
        return {
            name.lower(): column.model_copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }


class DbtCatalog(BaseModel):
    '''A dbt catalog'''

    nodes: Dict[str, DbtCatalogNode]


class DbtModelColumnMeta(BaseModel):
    '''Metadata about a column in a dbt model'''

    looker: Optional[DbtMetaLooker] = DbtMetaLooker()


class DbtModelColumn(BaseModel):
    '''A column in a dbt model'''

    name: str
    lookml_long_name: Optional[str] = ''
    lookml_name: Optional[str] = ''
    description: Optional[str] = None
    data_type: Optional[str] = None
    inner_types: list[str] = []
    meta: Optional[DbtModelColumnMeta] = DbtModelColumnMeta()
    nested: Optional[bool] = False
    is_primary_key: Optional[bool] = False

    # Root validator
    @model_validator(mode="before")
    @classmethod
    def set_nested_and_parent_name(cls, values):
        name = values.get('name', '')

        # If there's a dot in the name, it's a nested field
        if '.' in name:
            values['nested'] = True
        values['name'] = name.lower()
        values['lookml_long_name'] = name.replace('.', '__').lower()
        values['lookml_name'] = name.split('.')[-1].lower()
        values['description'] = values.get('description', "This field is missing a description.")
        # If the field is an array, it's a nested field
        return values

    @model_validator(mode="before")
    @classmethod
    def set_primary_key(cls, values):
        constraints = values.get('constraints', [])

        # if there is a primary key in constraints
        if {'type': 'primary_key'} in constraints:
            logging.debug('Found primary key on %s model', values['name'])
            values['is_primary_key'] = True

        return values


class DbtModelMeta(BaseModel):
    '''Metadata about a dbt model'''

    looker: Optional[DbtMetaLooker] = DbtMetaLooker()


class DbtModel(DbtNode):
    """A dbt model representing a SQL transformation.

    Contains information about the model's structure, columns, and metadata.
    """

    resource_type: Literal['model']
    relation_name: str
    db_schema: str = Field(..., alias='schema')
    name: str
    description: str
    columns: Dict[str, DbtModelColumn]
    tags: List[str]
    meta: DbtModelMeta
    path: str

    @field_validator('columns')
    @classmethod
    def case_insensitive_column_names(cls, v: Dict[str, DbtModelColumn]):
        """Convert all column names to lowercase for case-insensitive matching."""
        new_columns = {}
        for name, column in v.items():
            # Skip the entry if the name is not a string
            if not isinstance(name, str):
                raise TypeError(f"The key {name} is not a string.")
            # Skip the entry if the column is a dict, not a DbtModelColumn instance
            if isinstance(column, dict):
                new_columns[name] = column
            elif isinstance(column, DbtModelColumn):
                # Lowercase the name and update the column name
                new_columns[name.lower()] = column.model_copy(update={'name': column.name.lower()})
            else:
                raise TypeError(f"The value for key {name} is not a DbtModelColumn instance.")
        return new_columns


class DbtManifestMetadata(BaseModel):
    """Metadata about a dbt manifest.

    Contains information about the dbt adapter type and ensures it's supported.
    """

    adapter_type: str

    @field_validator('adapter_type')
    @classmethod
    def adapter_must_be_supported(cls, v):
        """Validate that the adapter type is supported."""
        if v not in enums.SupportedDbtAdapters:
            raise UnsupportedDbtAdapterError(
                f'Adapter type {v} is not supported. '
                f'Supported adapters are: {[e.value for e in enums.SupportedDbtAdapters]}'
            )
        return v


class DbtManifest(BaseModel):
    """A dbt manifest containing nodes, metadata, and exposures.

    The manifest is the main entry point for accessing dbt project information.
    """

    nodes: Dict[str, Union[DbtModel, DbtNode]]
    metadata: DbtManifestMetadata
    exposures: Dict[str, DbtExposure]
