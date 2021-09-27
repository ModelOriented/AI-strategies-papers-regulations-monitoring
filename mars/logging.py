import traceback
from logging import *
from logging import Logger


def log_exception(text: str, exception: Exception, logger: Logger) -> None:
    logger.info("%s %s" % (text, exception))
    logger.debug(traceback.format_exc())
