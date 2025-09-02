# Vulture whitelist for dbt2lookml project
# This file contains code patterns that vulture should ignore

# Enum values that are used dynamically or by external systems
INT64
INTEGER
FLOAT
FLOAT64
NUMERIC
DECIMAL
BIGNUMERIC
BOOLEAN
STRING
TIMESTAMP
DATETIME
DATE
TIME
BOOL
GEOGRAPHY
BYTES
ARRAY
STRUCT
NUMBER
YESNO
RAW
WEEK
MONTH
QUARTER
YEAR
MANY_TO_ONE
MANY_TO_MANY
ONE_TO_ONE
ONE_TO_MANY
LEFT_OUTER
FULL_OUTER
INNER
CROSS
SEED
SNAPSHOT
TEST
ANALYSIS
OPERATION
EXPOSURE
MACRO
RPC
SQL_OPERATION
SOURCE
DOC
GROUP
METRIC
SAVED_QUERY
SEMANTIC_MODEL
UNIT_TEST
FIXTURE

# Pydantic model fields that appear unused but are used by the framework
package
macros
url
owner
index
relationships
adapter_type
filename
can_filter
join_model
sql_on
relationship

# Exception classes that may not be explicitly raised in analyzed code
NotImplementedError
LookMLValidationError

# Validation functions that may be used by external tools
validate_generated_lookml

# Model classes that may be used dynamically
DbtCatalogNodeRelationship
LookViewFile

# Validator functions used by Pydantic
yes_no_validator

# Method used for validation
validate_measure_attributes

# Test variables that may appear unused but are part of test setup
expected_value
mock_logging
lookml_v1
lookml_v3

# Compiled regex patterns used dynamically
regex_patterns_compiled

# Block type variables used in parsing
current_block_type

# Template variables
msg_template
