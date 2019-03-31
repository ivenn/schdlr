import logging
import sys


def get_logger(name, stdout=True):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if stdout:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)-16s - %(threadName)-8s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger


class _undef_:
    pass

