import time


class _undef_:
    pass


PRIORITY_LOW = 3
PRIORITY_MID = 2
PRIORITY_HI = 1


class Task:

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"

    def __init__(self, name, func, args, kwargs, priority=PRIORITY_MID):
        self.name = name
        self.priority = priority
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._status = self.PENDING
        self._events = {self.PENDING: time.time()}  # tracking status change
        self._result = _undef_

    def __repr__(self):
        return "Task(name={name}, func={func}, args={args}," \
               " kwargs={kwargs}, status={status})".format(
                name=self.name, func=self._func.__name__,
                args=self._args, kwargs=self._kwargs, status=self.status
            )

    def _log_event(self, event):
        self._events[event] = time.time()

    @property
    def log(self):
        return self._events

    @property
    def failed(self):
        return isinstance(self._result, Exception)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        self._log_event(new_status)
        self._status = new_status

    @property
    def result(self):
        return self._result

    @property
    def done(self):
        return self.status == self.DONE and self.result is not _undef_

    def execute(self):
        self.status = self.IN_PROGRESS
        try:
            self._result = self._func(*self._args, **self._kwargs)
        except Exception as e:
            self._result = e
        finally:
            self.status = self.DONE


