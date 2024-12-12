class CliError(Exception):
    """Base exception for CLI errors"""

    pass


class NotImplementedError(CliError):
    pass


class UnsupportedDbtAdapterError(ValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter, only bigquery is supported.'
