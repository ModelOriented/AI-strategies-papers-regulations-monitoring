import traceback
from logging import *


def log_exception(text, exception, logger):
    logger.info("%s %s" % (text, exception))
    logger.debug(traceback.format_exc())
