import time

from .worker import Worker
from .task import Task


def foo(a, b=1):
    time.sleep(a)
    return a+b


def test_simple():
    w = Worker()
    t = Task('foo_task', foo, (1,), {'b': 2})
    print(w.ready)
    w.start()
    print(w.ready)
    w.do(t)
    print(w.ready)
    time.sleep(1)
    print(w.ready)
    w.stop()
