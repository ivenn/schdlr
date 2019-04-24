import datetime

from schdlr.task import Task
from schdlr.workflow import Workflow


class Repeated:

    def __init__(self, proto, every):
        self.proto = proto
        self.every = datetime.timedelta(seconds=every)
        self.next_ts = datetime.datetime.now()
        self.repeated_num = 0

    def _new_one(self, name):
        raise NotImplementedError()

    def next(self):
        now = datetime.datetime.now()
        if self.next_ts < now:
            self.next_ts = now + self.every
            self.repeated_num += 1
            name = '{proto_name}-{number}'.format(
                proto_name=self.proto.name, number=self.repeated_num)
            return self._new_one(name)
        else:
            return None


class RepeatedTask(Repeated):

    def _new_one(self, name):
        return Task(name, self.proto.func, self.proto.args,
                    self.proto.kwargs, self.proto.timeout)


class RepeatedWorkflow(Repeated):

    def _new_one(self, name):
         raise NotImplementedError()  # TODO: implement


def to_repeat(item, every):
    if isinstance(item, Task):
        return RepeatedTask(item, every)
    elif isinstance(item, Workflow):
        return RepeatedWorkflow(item, every)
    else:
        raise Exception("Unable to repeat {item}".format(item=item))

