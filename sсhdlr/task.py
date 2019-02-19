import time


class _undef_:
    pass


PRIORITY_LOW = 3
PRIORITY_MID = 2
PRIORITY_HI = 1


class Task:

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"

    def __init__(self, name, func, args, kwargs, priority=PRIORITY_MID):
        self.name = name
        self.priority = priority
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._status = self.PENDING
        self._result = _undef_

    def __repr__(self):
        return "Task(func={func}, args={args}, kwargs={kwargs}, status={status})".format(
            func=self._func.__name__, args=self._args, kwargs=self._kwargs, status=self.status
        )

    @property
    def failed(self):
        return isinstance(self._result, Exception)

    @property
    def status(self):
        return self._status

    @property
    def result(self):
        return self._result

    @property
    def done(self):
        return self.status == self.DONE

    def execute(self):
        self._status = self.IN_PROGRESS
        try:
            self._result = self._func(*self._args, **self._kwargs)
        except Exception as e:
            self._result = e
        finally:
            self._status = self.DONE


class TaskSet:

    def __init__(self, task, every):
        self.task = task
        self.every = every  # in sec
        self._prev_ts = None

    def get(self, number=1):
        now = time.time()
        if not self._prev_ts or now > self._prev_ts + self.every:
            self._prev_ts = now
            return self.task
        else:
            return None

