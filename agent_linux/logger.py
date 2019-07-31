import logging
import os
import sys
from logging.handlers import RotatingFileHandler


DEFAULT_LOG = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs/agent.log')


def setup_logger(logger, logfile, max_bytes, backup_count):
    """Sets up logging and associated handlers."""

    logger.setLevel(logging.DEBUG)

    logdir = os.path.dirname(logfile)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    formatter = logging.Formatter('%(asctime)s %(name)s[%(thread)d] '
                                             '%(levelname)s: %(message)s')

    rfhandler = RotatingFileHandler(filename=logfile, maxBytes=max_bytes, backupCount=backup_count)
    rfhandler.setLevel(logging.DEBUG)
    rfhandler.setFormatter(formatter)

    consolehandler = logging.StreamHandler(sys.stdout)
    consolehandler.setLevel(logging.DEBUG)
    consolehandler.setFormatter(formatter)

    logger.addHandler(rfhandler)
    logger.addHandler(consolehandler)


def get_logger(name, logfile=DEFAULT_LOG, max_bytes=10 * 1024 * 1024, backup_count=20):
    logger = logging.getLogger(name)
    setup_logger(logger, logfile, max_bytes, backup_count)
    return logger
