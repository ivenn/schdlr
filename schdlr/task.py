import time
import traceback


class _undef_:
    pass


PRIORITY_LOW = 3
PRIORITY_MID = 2
PRIORITY_HI = 1
DEFAULT_PRIORITY = PRIORITY_MID

PENDING = "PENDING"
SCHEDULED = "SCHEDULED"
IN_PROGRESS = "IN_PROGRESS"
FAILED = "FAILED"
DONE = "DONE"


class Task:

    def __init__(self, name, func, args, kwargs, timeout=None):
        self.name = name
        self.timeout = timeout
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._priority = DEFAULT_PRIORITY
        self._status = PENDING
        self._events = {PENDING: time.time()}  # tracking status change
        self.result = _undef_
        self.traceback = _undef_

    def __repr__(self):
        #return "Task(name={name}, func={func}, status={status})".format(
        #        name=self.name, func=self._func.__name__, status=self.status
        #)
        return "Task(name={name})".format(name=self.name)

    def __lt__(self, other):
        # to be handled correctly by PriorityQueue
        return self.priority < other.priority

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, priority):
        self._log_event("priority:{old}->{new}".format(
            old=self.priority, new=priority
        ))
        self._priority = int(priority)

    def _log_event(self, event):
        self._events[event] = time.time()

    @property
    def log(self):
        return self._events

    @property
    def failed(self):
        return self._status == FAILED

    @property
    def done(self):
        return self._status == DONE

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        self._log_event(new_status)
        self._status = new_status

    @property
    def timed_out(self):
        if self.timeout and self.status == IN_PROGRESS:
            return time.time() > self._events[IN_PROGRESS] + self.timeout
        return False

    def execute(self):
        self.status = IN_PROGRESS
        try:
            self.result = self._func(*self._args, **self._kwargs)
            self.status = DONE
        except Exception as e:
            self.result = e
            self.traceback = traceback.format_exc()
            self.status = FAILED
