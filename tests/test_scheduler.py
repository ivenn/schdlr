import time

from schdlr.scheduler import Scheduler, schdlr
from schdlr.task import Task, _undef_, PRIORITY_LOW, PRIORITY_MID, PRIORITY_HI


def foo(to_return, to_sleep=0, to_raise=None):
    if to_sleep:
        time.sleep(to_sleep)
    if to_raise:
        raise to_raise
    return to_return


def check_scheduler_state(schdlr, exp_state,
                          exp_tasks_in_queue, exp_in_progress, exp_tasks_done,
                          waiting_for_worker):

    assert schdlr.state == exp_state, "{act} != {exp}".format(act=schdlr.state, exp=exp_state)
    exp_tasks_stat = {'in_queue': exp_tasks_in_queue,
                      'in_progress': exp_in_progress,
                      'waiting_for_worker': waiting_for_worker,
                      'done': exp_tasks_done}
    act_task_state = schdlr.tasks_stat(short=True)
    assert act_task_state == exp_tasks_stat, "{act} != {exp}".format(act=act_task_state, exp=exp_tasks_stat)


def compare_task_list(exp_list, act_list):
    assert act_list == exp_list, "{act} != {exp}".format(act=act_list, exp=exp_list)


def test_no_tasks_start_stop():
    schdlr = Scheduler(1)

    check_scheduler_state(schdlr, Scheduler.STATE_NOT_STARTED, 0, 0, 0, None)

    schdlr.start()
    check_scheduler_state(schdlr, Scheduler.STATE_RUNNING, 0, 0, 0, None)

    schdlr.stop()
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 0, None)


def test_one_worker_one_task_pass_added_before_start():
    schdlr = Scheduler(1)
    task = Task('test-1', foo, (1,), {'to_sleep': 0.3, })

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
    task = Task('test-1', foo, (1,), {'to_sleep': 0.3, })

    assert task.done is False and task.result is _undef_
    schdlr.start()

    schdlr.add_task(task)
    time.sleep(0.1)
    check_scheduler_state(schdlr, Scheduler.STATE_RUNNING, 0, 1, 0, None)
    schdlr.stop()
    assert task.done is True and task.result == 1
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 1, None)


def test_multiple_task_order_same_priority():
    schdlr = Scheduler(1)

    task1 = Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = Task('test-3', foo, (1,), {'to_sleep': 0.3, })

    for task in [task1, task2, task3]:
        schdlr.add_task(task)

    schdlr.start()
    time.sleep(0.5)
    schdlr.stop()

    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 3, None)
    assert schdlr.tasks_stat(short=False)["done"] == [task1, task2, task3]


def test_multiple_task_order_different_priority():
    schdlr = Scheduler(1)

    task1 = Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = Task('test-3', foo, (1,), {'to_sleep': 0.3, })

    schdlr.add_task(task1, priority=PRIORITY_LOW)
    schdlr.add_task(task2, priority=PRIORITY_MID)
    schdlr.add_task(task3, priority=PRIORITY_HI)

    schdlr.start()
    time.sleep(0.5)
    schdlr.stop()

    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 3, None)
    compare_task_list(schdlr.tasks_stat(short=False)["done"],
                      [task3, task2, task1])


def test_multiple_task_order_different_priority_2():
    schdlr = Scheduler(1)

    task1 = Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = Task('test-3', foo, (1,), {'to_sleep': 0.3, })
    task4 = Task('test-4', foo, (1,), {'to_sleep': 0.3, })

    schdlr.add_task(task1, priority=PRIORITY_MID)
    schdlr.add_task(task2, priority=PRIORITY_MID)
    schdlr.add_task(task3, priority=PRIORITY_MID)

    schdlr.start()
    time.sleep(0.1)
    schdlr.add_task(task4, priority=PRIORITY_HI)
    schdlr.stop()

    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 4, None)
    compare_task_list(schdlr.tasks_stat(short=False)["done"],
                      [task1, task2, task4, task3])


def test_overdue_tasks():
    schdlr = Scheduler(4)

    task1 = Task('test-1', foo, (1,), {'to_sleep': 1, })
    task2 = Task('test-2', foo, (1,), {'to_sleep': 1, })
    task3 = Task('test-3', foo, (1,), {'to_sleep': 1, })

    schdlr.start()
    schdlr.add_task(task1, priority=PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 1

    schdlr.add_task(task2, priority=PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 2

    schdlr.add_task(task3, priority=PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 3

    time.sleep(1)

    assert len(schdlr.overdue_tasks) == 0

    schdlr.stop()


def test_complete_enqueued():
    schdlr = Scheduler(4)

    task1 = Task('test-1', foo, (1,), {'to_sleep': 1, })
    task2 = Task('test-2', foo, (1,), {'to_sleep': 1, })
    task3 = Task('test-3', foo, (1,), {'to_sleep': 1, })

    schdlr.start()
    schdlr.add_task(task1)
    schdlr.add_task(task2)
    schdlr.add_task(task3)

    schdlr.stop()
    check_scheduler_state(schdlr, Scheduler.STATE_STOPPED, 0, 0, 3, None)


def test_schdlr_context_manager():
    with schdlr(1) as scheduler:
        scheduler.add_task(Task('test-1', foo, (1,), {'to_sleep': 1, }))


