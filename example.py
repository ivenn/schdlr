import time

from schdlr.scheduler import Scheduler
from schdlr.task import Task


def foo(a, b=1):
    time.sleep(a)
    #print(a)
    return a+b


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



#for t in tasks:
#    print(t.log)

