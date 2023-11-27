"""
log_config.py
~~~~~~~~~~~~~

Exports a simple logger configuration for the root logger.
"""
import logging
import os
from datetime import datetime

CONSOLE_LOG_FORMAT = ('%(levelname)-8s | %(threadName)-15s | '
                      '%(filename)s:%(lineno)s | %(message)s')
FILE_LOG_FORMAT = ('%(asctime)s | %(levelname)-8s | %(threadName)-15s '
                   '%(filename)s:%(lineno)s | %(message)s')
FILENAME_FORMAT = '%Y-%m-%dT%H-%M-%S-%f'
LOG_FILE_EXTENSION = '.log'


def config_root_logger(log_folder: str) -> None:
    """ Configure the root logger with console handler and file handler.

    Args:
        log_folder: The log folder to put the log file into.
    """

    # Create console handlers
    c_handler = logging.StreamHandler()
    c_format = logging.Formatter(CONSOLE_LOG_FORMAT)
    c_handler.setFormatter(c_format)
    c_handler.setLevel(logging.INFO)

    # Create file handler
    if not os.path.isdir(log_folder):
        os.makedirs(log_folder)
    filename = datetime.now().strftime(FILENAME_FORMAT) + LOG_FILE_EXTENSION
    f_handler = logging.FileHandler(os.path.join(log_folder, filename))
    f_format = logging.Formatter(FILE_LOG_FORMAT)
    f_handler.setFormatter(f_format)
    f_handler.setLevel(logging.DEBUG)

    # Add handlers to the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
