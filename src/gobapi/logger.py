import logging
import os

from gobcore.logging.logger import Logger


def get_logger(name):
    run_mode = os.getenv('GOB_RUN_MODE', 'PRODUCTION')
    if run_mode == 'TEST':
        return logging.getLogger(name)
    return Logger(name)
