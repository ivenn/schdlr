import datetime

from schdlr.task import Task
from schdlr.workflow import Workflow


class Repeated:

    def __init__(self, proto, every, limit):
        self.proto = proto
        self.limit = limit
        self.counter = 0
        self.every = datetime.timedelta(seconds=every)
        self.next_ts = datetime.datetime.now()

    def _new_one(self):
        raise NotImplementedError()

    def next(self):
        now = datetime.datetime.now()
        if not self.limit or self.counter < self.limit:
            if self.next_ts < now:
                self.next_ts = now + self.every
                self.counter += 1
                return self._new_one()

        return None


class RepeatedTask(Repeated):

    def _new_one(self):
        return self.proto.copy()


class RepeatedWorkflow(Repeated):

    def _new_one(self):
        raise NotImplementedError()  # TODO: implement


def to_repeat(item, every, limit=None):
    if isinstance(item, Task):
        return RepeatedTask(item, every, limit)
    elif isinstance(item, Workflow):
        return RepeatedWorkflow(item, every, limit)
    else:
        raise Exception("Unable to repeat {item}".format(item=item))

