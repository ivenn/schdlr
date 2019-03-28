import time

from schdlr import scheduler
from schdlr import task


def foo(to_return, to_sleep=0, to_raise=None):
    if to_sleep:
        time.sleep(to_sleep)
    if to_raise:
        raise to_raise
    return to_return


def check_scheduler_state(scheduler, exp_state,
                          exp_tasks_in_queue, exp_in_progress, exp_tasks_done,
                          waiting_for_worker):

    assert scheduler.state == exp_state, "{act} != {exp}".format(act=scheduler.state, exp=exp_state)
    exp_tasks_stat = {'in_queue': exp_tasks_in_queue,
                      'in_progress': exp_in_progress,
                      'waiting_for_worker': waiting_for_worker,
                      'done': exp_tasks_done}
    act_task_state = scheduler.tasks_stat(short=True)
    assert act_task_state == exp_tasks_stat, "{act} != {exp}".format(act=act_task_state, exp=exp_tasks_stat)


def compare_task_list(exp_list, act_list):
    assert act_list == exp_list, "{act} != {exp}".format(act=act_list, exp=exp_list)


def test_no_tasks_start_stop():
    schdlr = scheduler.Scheduler(1)

    check_scheduler_state(schdlr, scheduler.STATE_NOT_STARTED, 0, 0, 0, None)

    schdlr.start()
    check_scheduler_state(schdlr, scheduler.STATE_RUNNING, 0, 0, 0, None)

    schdlr.stop()
    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 0, None)


def test_one_worker_one_task_pass_added_before_start():
    schdlr = scheduler.Scheduler(1)
    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 0.3, })

    schdlr.add_task(task1)
    assert task1.done is False and task1.result is task._undef_
    schdlr.start()
    time.sleep(0.1)
    check_scheduler_state(schdlr, scheduler.STATE_RUNNING, 0, 1, 0, None)
    schdlr.stop()
    assert task1.done is True and task1.result == 1
    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 1, None)


def test_one_worker_one_task_pass_added_after_start():
    schdlr = scheduler.Scheduler(1)
    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 0.3, })

    assert task1.done is False and task1.result is task._undef_
    schdlr.start()

    schdlr.add_task(task1)
    time.sleep(0.1)
    check_scheduler_state(schdlr, scheduler.STATE_RUNNING, 0, 1, 0, None)
    schdlr.stop()
    assert task1.done is True and task1.result == 1
    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 1, None)


def test_multiple_task_order_same_priority():
    schdlr = scheduler.Scheduler(1)

    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = task.Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = task.Task('test-3', foo, (1,), {'to_sleep': 0.3, })

    for t in [task1, task2, task3]:
        schdlr.add_task(t)

    schdlr.start()
    time.sleep(0.5)
    schdlr.stop()

    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 3, None)
    assert schdlr.tasks_stat(short=False)["done"] == [task1, task2, task3]


def test_multiple_task_order_different_priority():
    schdlr = scheduler.Scheduler(1)

    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = task.Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = task.Task('test-3', foo, (1,), {'to_sleep': 0.3, })

    schdlr.add_task(task1, priority=task.PRIORITY_LOW)
    schdlr.add_task(task2, priority=task.PRIORITY_MID)
    schdlr.add_task(task3, priority=task.PRIORITY_HI)

    schdlr.start()
    time.sleep(0.5)
    schdlr.stop()

    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 3, None)
    compare_task_list(schdlr.tasks_stat(short=False)["done"],
                      [task3, task2, task1])


def test_multiple_task_order_different_priority_2():
    schdlr = scheduler.Scheduler(1)

    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 0.3, })
    task2 = task.Task('test-2', foo, (1,), {'to_sleep': 0.3, })
    task3 = task.Task('test-3', foo, (1,), {'to_sleep': 0.3, })
    task4 = task.Task('test-4', foo, (1,), {'to_sleep': 0.3, })

    schdlr.add_task(task1, priority=task.PRIORITY_MID)
    schdlr.add_task(task2, priority=task.PRIORITY_MID)
    schdlr.add_task(task3, priority=task.PRIORITY_MID)

    schdlr.start()
    time.sleep(0.1)
    schdlr.add_task(task4, priority=task.PRIORITY_HI)
    schdlr.stop()

    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 4, None)
    compare_task_list(schdlr.tasks_stat(short=False)["done"],
                      [task1, task2, task4, task3])


def test_overdue_tasks():
    schdlr = scheduler.Scheduler(4)

    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 1, })
    task2 = task.Task('test-2', foo, (1,), {'to_sleep': 1, })
    task3 = task.Task('test-3', foo, (1,), {'to_sleep': 1, })

    schdlr.start()
    schdlr.add_task(task1, priority=task.PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 1

    schdlr.add_task(task2, priority=task.PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 2

    schdlr.add_task(task3, priority=task.PRIORITY_MID, timeout=0.1)
    time.sleep(0.2)
    assert len(schdlr.overdue_tasks) == 3

    time.sleep(1)

    assert len(schdlr.overdue_tasks) == 0

    schdlr.stop()


def test_complete_enqueued():
    schdlr = scheduler.Scheduler(4)

    task1 = task.Task('test-1', foo, (1,), {'to_sleep': 1, })
    task2 = task.Task('test-2', foo, (1,), {'to_sleep': 1, })
    task3 = task.Task('test-3', foo, (1,), {'to_sleep': 1, })

    schdlr.start()
    schdlr.add_task(task1)
    schdlr.add_task(task2)
    schdlr.add_task(task3)

    schdlr.stop()
    check_scheduler_state(schdlr, scheduler.STATE_STOPPED, 0, 0, 3, None)



