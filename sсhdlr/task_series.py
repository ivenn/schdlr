import time


class TaskSeries:

    def __init__(self, task, every):
        self.task = task
        self.every = every  # in sec
        self._prev_ts = None

    def next(self, number=1):
        now = time.time()
        if not self._prev_ts or now > self._prev_ts + self.every:
            self._prev_ts = now
            return self.task
        else:
            return None