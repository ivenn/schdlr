import time

from schdlr.scheduler import Scheduler
from schdlr.task import Task


def foo(a, b=1):
    time.sleep(a)
    #print(a)
    return a+b


def example():
    s = Scheduler(20)
    tt = Task('task', foo, (7,), {'b': 2})

    tasks = [
        Task('task11', foo, (1,), {'b': 2}),
        Task('task22', foo, (3,), {'b': 2}),
        Task('task33', foo, (10,), {'b': 2}),
        Task('task44', foo, (1,), {'b': 2}),
        Task('task55', foo, (3,), {'b': 2}),
    ]
    tasks = [Task('task%s' % i, foo, (1,), {'b': 2}) for i in range(100)]

    start = time.time()
    s.start()
    print(s)
    for t in tasks:
        s.add_task(t)

    s.stop()
    print(time.time() - start)


def example2():
    s = Scheduler(3, monitor=True)

    tasks = [Task('task%s' % i, foo, (20,), {'b': 2}, timeout=10) for i in range(5)]

    start = time.time()
    s.start()
    for t in tasks:
        s.add_task(t, 10)
    time.sleep(5)
    print(s.overdue_tasks)
    time.sleep(6)
    print(s.overdue_tasks)
    print(time.time() - start)


example2()




