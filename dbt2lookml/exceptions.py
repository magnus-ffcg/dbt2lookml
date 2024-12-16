from typing import Optional


class CliError(Exception):
    """Base exception for CLI errors.

    This exception is raised when there are errors during CLI operations,
    such as file I/O errors or parsing errors.

    Attributes:
        message: The error message
        details: Optional additional error details
    """

    def __init__(self, message: str, details: Optional[str] = None):
        self.message = message
        self.details = details
        super().__init__(self.format_message())

    def format_message(self) -> str:
        """Format the error message with optional details."""
        return f"{self.message} - {self.details}" if self.details else self.message


class NotImplementedError(CliError):
    pass


class UnsupportedDbtAdapterError(ValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter, only bigquery is supported.'
