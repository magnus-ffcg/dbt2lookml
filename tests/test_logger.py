import pytest
import logging

from dbt2lookml.log import Logger

class Fixture(Logger):
    def output(self, value):
        self._logger.info(value)
        return value

class TestLogger:

    def test_class_logging(self, mocker):
        """Test that logger is properly initialized"""
        fixture = Fixture()
        
        assert fixture._logger is not None
        assert fixture._logger.name == "dbt2lookml.log"
        
        mock_logger = mocker.patch.object(fixture._logger, 'info')
        
        assert fixture.output('testing logger') == 'testing logger'

        mock_logger.assert_called_once_with('testing logger')

    def test_logger_initialization(self):
        """Test that logger is properly initialized"""
        logger = Logger()
        assert logger._logger is not None
        assert logger._logger.name == "dbt2lookml.log"

    def test_set_loglevel(self):
        """Test setting log level"""
        logger = Logger()
        logger.set_loglevel('DEBUG')
        assert logger._logger.level == logging.DEBUG
        
        logger.set_loglevel('INFO')
        assert logger._logger.level == logging.INFO
        
        logger.set_loglevel('WARNING')
        assert logger._logger.level == logging.WARNING
        
        logger.set_loglevel('ERROR')
        assert logger._logger.level == logging.ERROR