import pytest
from dbt2lookml.enums import (
    ExtendedEnum,
    SupportedDbtAdapters,
    LookerMeasureType,
    LookerValueFormatName,
    LookerTimeFrame,
    LookerBigQueryDataType,
    LookerBigQueryMeasureTypes,
    LookerDateTimeTypes,
    LookerDateTypes,
    LookerScalarTypes,
    LookerDateTimeframes,
    LookerTimeTimeframes
)


class TestEnums:
    def test_enum_values(self):
        assert LookerScalarTypes.values() == ['number', 'yesno', 'string']
        assert LookerMeasureType.values() == ['number', 'string', 'average', 'average_distinct', 'count', 'count_distinct', 'list', 'max', 'median', 'median_distinct', 'min', 'sum', 'sum_distinct']
        
    def test_enum_value(self):
        assert LookerBigQueryDataType.get("INT64") == "number"
        assert LookerBigQueryDataType.get("BOOLEAN") == "yesno"

    def test_extended_enum_values(self):
        """Test ExtendedEnum values() method"""
        assert LookerMeasureType.values() == [
            'number', 'string', 'average', 'average_distinct', 'count', 
            'count_distinct', 'list', 'max', 'median', 'median_distinct', 
            'min', 'sum', 'sum_distinct'
        ]

    def test_extended_enum_get(self):
        """Test ExtendedEnum get() method"""
        assert LookerMeasureType.get('NUMBER') == 'number'
        assert LookerMeasureType.get('COUNT') == 'count'
        assert LookerMeasureType.get('INVALID') is None

    def test_supported_dbt_adapters(self):
        """Test SupportedDbtAdapters enum"""
        assert SupportedDbtAdapters.BIGQUERY.value == 'bigquery'
        assert SupportedDbtAdapters.get('BIGQUERY') == 'bigquery'

    def test_looker_bigquery_data_type(self):
        """Test LookerBigQueryDataType enum"""
        assert LookerBigQueryDataType.get('INT64') == 'number'
        assert LookerBigQueryDataType.get('STRING') == 'string'
        assert LookerBigQueryDataType.get('BOOLEAN') == 'yesno'

    def test_looker_value_format_name(self):
        """Test LookerValueFormatName enum"""
        assert LookerValueFormatName.get('DECIMAL_0') == 'decimal_0'
        assert LookerValueFormatName.get('USD') == 'usd'
        assert LookerValueFormatName.get('ID') == 'id'

    def test_looker_time_frame(self):
        """Test LookerTimeFrame enum"""
        assert LookerTimeFrame.get('DATE') == 'date'
        assert LookerTimeFrame.get('HOUR') == 'hour'
        assert LookerTimeFrame.get('MINUTE') == 'minute'

    def test_looker_date_time_types(self):
        """Test LookerDateTimeTypes enum"""
        assert LookerDateTimeTypes.get('DATETIME') == 'datetime'
        assert LookerDateTimeTypes.get('TIMESTAMP') == 'timestamp'

    def test_looker_date_types(self):
        """Test LookerDateTypes enum"""
        assert LookerDateTypes.get('DATE') == 'date'

    def test_looker_scalar_types(self):
        """Test LookerScalarTypes enum"""
        assert LookerScalarTypes.get('NUMBER') == 'number'
        assert LookerScalarTypes.get('STRING') == 'string'
        assert LookerScalarTypes.get('YESNO') == 'yesno'