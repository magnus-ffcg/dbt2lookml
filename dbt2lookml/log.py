import logging
from rich.logging import RichHandler

# default logging level
logging.basicConfig(
    level=getattr(logging, 'INFO'), format="%(message)s", handlers=[RichHandler(rich_tracebacks=True)]
)

class Logger():
    """Logger class to extend classes with logging capabilities"""
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        
    def set_loglevel(self, level):
        self._logger.setLevel(getattr(logging, level))
        