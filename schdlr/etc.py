import logging
import sys
import time


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


class Stateful:

    def __init__(self, initial_state, logger):
        self._state = initial_state
        self._events = {self._state: time.time()}
        self._logger = logger

    @property
    def events(self):
        return self._events

    def _log_event(self, event):
        self._events[event] = time.time()

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        self._log_event(new_state)
        self._logger.info("{old} -> {new}".format(old=self._state, new=new_state))
        self._state = new_state
