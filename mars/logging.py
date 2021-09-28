import traceback
from logging import *
from logging import Logger, basicConfig, getLevelName, getLogger

import mars.config


def log_exception(text: str, exception: Exception, logger: Logger) -> None:
    logger.info("%s %s" % (text, exception))
    logger.debug(traceback.format_exc())


def new_logger(name: str) -> Logger:
    logger = getLogger(name)
    level = getLevelName(mars.config.logging_level)
    logger.setLevel(level)
    basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")
    return logger
