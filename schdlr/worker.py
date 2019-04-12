from queue import Queue
from threading import Thread, Event

from schdlr import task
from schdlr import etc


class WorkerStop(Exception):
    pass


NOT_STARTED = "NOT_STARTED"
IDLE = "WAITING TASK"
PROCESSING = "PROCESSING"
STOPPING = "STOPPING"
STOPPED = "STOPPED"


class Worker:

    def __init__(self, name):
        self.name = name
        self._t = None
        self._terminated = None
        self._inbox = Queue()
        self._ready = Event()
        self._state = NOT_STARTED
        self.task_in_progress = None
        self.tasks_done = []
        self.logger = etc.get_logger("worker.{name}".format(name=self.name))

    def __repr__(self):
        return "Worker({name}): {state}".format(
            name=self.name, state=self._state)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        self.logger.info("{old} -> {new}".format(old=self._state, new=new_state))
        self._state = new_state
        if new_state in [STOPPING, STOPPED]:
            self.ready = False

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
        self.state = STOPPING
        if wait:
            self._terminated.wait()
        self.state = STOPPED

    def _loop(self):
        try:
            while True:
                self.state = IDLE
                tsk = self._inbox.get()

                if tsk is WorkerStop:
                    break
                elif isinstance(tsk, task.Task):
                    self.logger.info("Processing %s" % tsk)
                    self.state = PROCESSING
                    self.task_in_progress = tsk
                    tsk.execute()
                    self.task_in_progress = None
                    self.tasks_done.append(tsk)
                    self.logger.info("%s is done" % tsk)
                else:
                    self.logger.info("Unexpected task: %s" % tsk)

                if self._inbox.empty():
                    self.ready = True
        finally:
            self._terminated.set()

    def do(self, tsk, block=True):
        if block:
            self.ready = False
        self._inbox.put(tsk)
