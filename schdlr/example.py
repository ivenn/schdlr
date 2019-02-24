import time

from schdlr.scheduler import Scheduler
from schdlr.task import Task
from schdlr.workflow import Workflow


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

def error_function():
    raise Exception("Test Exception")

error_task = Task("error_task", error_function)

s.run()
print(s)
for t in tasks:
    s.add_task(t)
time.sleep(10)
s.stop()
s.add_task(tt)


for t in tasks:
    print(t.log)

# Workflow part

first_workflow = Workflow("first_workflow")
for task in tasks:
    first_workflow.add_task(task)
print(first_workflow)
first_workflow.execute()
print(first_workflow.last_task)
print(first_workflow.last_result)

## Failed workflow
second_workflow = Workflow("second_workflow")
for task in tasks[:-1]:
    second_workflow.add_task(task)
second_workflow.add_task(error_task)
second_workflow.add_task(tasks[-1])
print(second_workflow)
second_workflow.execute()
print(second_workflow.last_result)
print(second_workflow)
print(second_workflow.last_task.log)