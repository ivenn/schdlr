import time

from .scheduler import Scheduler
from .task import Task, _undef_


def foo(to_return, to_sleep=0, to_raise=None):
    if to_sleep:
        time.sleep(to_sleep)
    if to_raise:
        raise to_raise
    return to_return


def check_scheduler_state(schdlr, exp_state,
                          exp_tasks_in_queue, exp_in_progress, exp_tasks_done,
                          waiting_for_worker):

    assert schdlr.state == exp_state
    assert schdlr.tasks_stat(short=True) == {'in_queue': exp_tasks_in_queue,
                                             'in_progress': exp_in_progress,
                                             'waiting_for_worker': waiting_for_worker,
                                             'done': exp_tasks_done}


def test_no_tasks_start_stop():
    schdlr = Scheduler(1)

    check_scheduler_state(schdlr, Scheduler.STATE_NOT_STARTED, 0, 0, 0, None)

    schdlr.start()
    check_scheduler_state(schdlr, Scheduler.STATE_RUNNING, 0, 0, 0, None)

    schdlr.stop()
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 0, None)


def test_one_worker_one_task_pass_added_before_start():
    schdlr = Scheduler(1)
    task = Task('test-1', foo, (1,), {'to_sleep': 1, })

    schdlr.add_task(task)
    assert task.done is False and task.result is _undef_
    schdlr.start()
    time.sleep(0.1)
    check_scheduler_state(schdlr, Scheduler.STATE_RUNNING, 0, 1, 0, None)
    schdlr.stop()
    assert task.done is True and task.result == 1
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 1, None)


def test_one_worker_one_task_pass_added_after_start():
    schdlr = Scheduler(1)
    task = Task('test-1', foo, (1,), {'to_sleep': 1, })

    assert task.done is False and task.result is _undef_
    schdlr.start()
    time.sleep(0.1)
    schdlr.add_task(task)
    check_scheduler_state(schdlr, Scheduler.STATE_RUNNING, 0, 0, 0, 'test-1')
    schdlr.stop()
    assert task.done is True and task.result == 1
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 1, None)
