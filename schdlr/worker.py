from queue import Queue
from threading import Thread, Event, RLock

from .task import Task
from .log import get_logger


log = get_logger(__name__)


class WorkerStop(Exception):
    pass


class Worker:

    NOT_STARTED = "NOT_STARTED"

    IDLE = "WAITING TASK"
    PROCESSING = "PROCESSING"

    STOPPING = "STOPPING"
    STOPPED = "STOPPED"

    def __init__(self, name):
        self.name = name
        self._t = None
        self._terminated = None
        self._inbox = Queue()
        self._status_lock = RLock()
        self._ready = Event()
        self._status = self.NOT_STARTED

    def __repr__(self):
        return "Worker({name}): {status}".format(
            name=self.name, status=self._status)

    @property
    def status(self):
        with self._status_lock:
            return self._status

    @status.setter
    def status(self, new_status):
        log.info("{old} -> {new}".format(old=self._status, new=new_status))
        with self._status_lock:
            if new_status in [self.STOPPING, self.STOPPED]:
                self.ready = False
            self._status = new_status

    @property
    def ready(self):
        return self._ready.is_set()

    @ready.setter
    def ready(self, ready):
        if ready:
            self._ready.set()
        else:
            self._ready.clear()

    def start(self):
        self._terminated = Event()
        self._t = Thread(target=self._loop, name=self.name.lower())
        self._t.daemon = True
        self._t.start()
        self.ready = True

    def stop(self, wait=False):
        self._inbox.put(WorkerStop)
        self.status = self.STOPPING
        if wait:
            self._terminated.wait()
        self.status = self.STOPPED

    def _loop(self):
        try:
            while True:
                self.status = self.IDLE
                task = self._inbox.get()

                if task is WorkerStop:
                    break
                elif isinstance(task, Task):
                    log.info("Processing %s" % task)
                    self.status = self.PROCESSING
                    task.execute()
                    log.info("%s is done" % task)
                else:
                    log.info("Unexpected task: %s" % task)

                if self._inbox.empty():
                    self.ready = True
        finally:
            self._terminated.set()

    def do(self, task, block=True):
        if block:
            self.ready = False
        self._inbox.put(task)
