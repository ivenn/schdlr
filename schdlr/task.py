import collections
import time
import traceback

from schdlr import etc


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

    tid_counter = 0  # task_id

    def __init__(self, name, timeout=None):
        self.tid = self.assign_tid()
        self.name = "{name}-{tid}".format(name=name, tid=self.tid)
        self.timeout = timeout
        self._priority = DEFAULT_PRIORITY
        self.result = etc._undef_
        self.traceback = None
        self._state = PENDING
        self._events = collections.OrderedDict({self._state: time.time()})
        self.logger = etc.get_logger("task.{name}".format(name=self.name))

    @classmethod
    def assign_tid(cls):
        cls.tid_counter += 1
        return cls.tid_counter

    def __repr__(self):
        return "{klass}(name={name}, state={state})".format(
            klass=self.__class__.__name__,
            name=self.name, state=self.state)

    def __lt__(self, other):
        # to be handled correctly by Scheduler's PriorityQueue
        return self.priority < other.priority

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
        self.logger.info("{old} -> {new}".format(old=self._state, new=new_state))
        self._state = new_state

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, priority):
        self._log_event("priority:{old}->{new}".format(
            old=self.priority, new=priority
        ))
        self._priority = int(priority)

    @property
    def failed(self):
        return self.state == FAILED

    @property
    def done(self):
        return self.state == DONE

    @property
    def timed_out(self):
        if self.timeout and self.state == IN_PROGRESS:
            return time.time() > self._events[IN_PROGRESS] + self.timeout
        return False

    def execute(self):
        self.state = IN_PROGRESS
        try:
            self.result = self.executable()
            self.state = DONE
        except Exception as e:
            self.result = e
            self.traceback = traceback.format_exc()
            self.state = FAILED

    def executable(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()


