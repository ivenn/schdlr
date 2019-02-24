import time

from schdlr.scheduler import Scheduler
from schdlr.task import Task


def foo(a, b=1):
    time.sleep(a)
    print(a)
    return a+b


s = Scheduler(2)
tt = Task('foo_task', foo, (7,), {'b': 2})

tasks = [
    Task('foo_task1', foo, (1,), {'b': 2}),
    Task('foo_task3', foo, (3,), {'b': 2}),
    Task('foo_task4', foo, (10,), {'b': 2}),
]

s.run()
print(s)
for t in tasks:
    s.add_task(t)
time.sleep(10)
s.stop()
s.add_task(tt)


for t in tasks:
    print(t.log)

