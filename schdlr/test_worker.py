import time

from .worker import Worker
from .task import Task


def foo(a, b=1):
    time.sleep(a)
    return a + b


def foo_raise():
    time.sleep(0.2)
    raise Exception("Test exception")


def check_worker_state(worker, exp_status, exp_ready):
    time.sleep(0.1)  # to let state change
    assert worker.status == exp_status
    assert worker.ready == exp_ready


def test_basic_states():
    worker = Worker('test_worker')
    check_worker_state(worker, worker.NOT_STARTED, False)
    worker.start()
    check_worker_state(worker, worker.IDLE, True)
    worker.stop()
    check_worker_state(worker, worker.STOPPED, False)


def test_worker_simple_pass():
    worker = Worker('test_worker')
    task = Task('foo_task', foo, (1,), {'b': 2})
    worker.start()
    worker.do(task)
    check_worker_state(worker, worker.PROCESSING, False)
    time.sleep(1)
    check_worker_state(worker, worker.IDLE, True)
    worker.stop()
    assert task.result == 3


def test_worker_simple_fail():
    worker = Worker('test_worker')
    task = Task('foo_task', foo_raise, (), {})
    worker.start()
    worker.do(task)
    check_worker_state(worker, worker.PROCESSING, False)
    time.sleep(1)
    check_worker_state(worker, worker.IDLE, True)
    worker.stop()
    assert task.failed


def test_worker_2_tasks():
    worker = Worker('test_worker')
    task1 = Task('foo_task1', foo_raise, (), {})
    task2 = Task('foo_task2', foo, (1,), {'b': 2})
    worker.start()
    worker.do(task1)
    worker.do(task2)
    time.sleep(3)
    worker.stop()
    assert task1.failed
    assert task2.result == 3



