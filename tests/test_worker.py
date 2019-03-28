import time

from schdlr import worker
from schdlr import task


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
    wkr = worker.Worker('test_worker')
    check_worker_state(wkr, worker.NOT_STARTED, False)
    wkr.start()
    check_worker_state(wkr, worker.IDLE, True)
    wkr.stop()
    check_worker_state(wkr, worker.STOPPED, False)


def test_worker_simple_pass():
    wkr = worker.Worker('test_worker')
    task1 = task.Task('foo_task', foo, (1,), {'b': 2})
    wkr.start()
    wkr.do(task1)
    check_worker_state(wkr, worker.PROCESSING, False)
    time.sleep(1)
    check_worker_state(wkr, worker.IDLE, True)
    wkr.stop()
    assert task1.result == 3


def test_worker_simple_fail():
    wkr = worker.Worker('test_worker')
    task1 = task.Task('foo_task', foo_raise, (), {})
    wkr.start()
    wkr.do(task1)
    check_worker_state(wkr, worker.PROCESSING, False)
    time.sleep(1)
    check_worker_state(wkr, worker.IDLE, True)
    wkr.stop()
    assert task1.failed


def test_worker_2_tasks():
    wkr = worker.Worker('test_worker')
    task1 = task.Task('foo_task1', foo_raise, (), {})
    task2 = task.Task('foo_task2', foo, (1,), {'b': 2})
    wkr.start()
    wkr.do(task1)
    wkr.do(task2)
    time.sleep(3)
    wkr.stop()
    assert task1.failed
    assert task2.result == 3



