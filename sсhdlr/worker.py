from queue import Queue
from threading import Thread, Event

from .task import Task
from .log import get_logger


log = get_logger(__name__)


class WorkerStop(Exception):
    pass


class Worker:

    NOT_STARTED = "NOT_STARTED"
    IDLE = "IDLE"
    PROCESSING = "PROCESSING"
    STOPPED = "STOPPED"

    def __init__(self, name):
        self.name = name
        self._t = None
        self._terminated = None
        self._inbox = Queue()
        self._status = self.NOT_STARTED

    def __repr__(self):
        return "Worker({name}): {status}".format(
            name=self.name, status=self._status)

    @property
    def status(self):
        return self._status

    @property
    def ready(self):
        return self._status == self.IDLE and self._inbox.empty()

    def start(self):
        self._terminated = Event()
        self._t = Thread(target=self._loop, name=self.name.lower())
        self._t.daemon = True
        self._t.start()

    def stop(self, wait=False):
        log.info('Stopping worker...')
        self._inbox.put(WorkerStop)
        if wait:
            self._terminated.wait()

    def _loop(self):
        self._status = self.IDLE
        try:
            while True:
                task = self._inbox.get()
                if task is WorkerStop:
                    raise WorkerStop()
                elif isinstance(task, Task):
                    log.info("Processing task %s" % task)
                    self._status = self.PROCESSING
                    task.execute()
                    self._status = self.IDLE
                    log.info("Task %s is done" % task)
                else:
                    log.info("Unexpected task: %s" % task)
        except WorkerStop:
            pass
        finally:
            self._terminated.set()
            log.info('Worker stopped...')

    def do(self, task):
        self._inbox.put(task)
