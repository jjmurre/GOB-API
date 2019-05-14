import logging
import os
from unittest.mock import patch

from gobcore.logging.logger import Logger
from gobapi.logger import get_logger


def test_run_test():
     assert isinstance(get_logger('a'), logging.Logger)


@patch("os.getenv")
def test_run_prod(mock):
    mock.return_value = 'PRODUCTION'
    assert isinstance(get_logger("a"), Logger)