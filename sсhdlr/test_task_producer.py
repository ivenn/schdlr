import time

from .task import Task, TaskSet


def foo(a, b=1, res=None):
    if isinstance(res, Exception):
        raise res
    else:
        return a, b, res


def test_simple():
    t = Task('foo_task', foo, (1,), {'b': 2, 'res': 3})
    print(t)
    tp = TaskSet(t, 2)
    assert isinstance(tp.get(), Task)
    time.sleep(1)
    assert tp.get() is None
    time.sleep(1)
    assert isinstance(tp.get(), Task)
