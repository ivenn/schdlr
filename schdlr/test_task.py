from .task import Task, _undef_


def foo(a, b=1, c=0):
    return a + b + c


def foo_raise(to_raise):
    raise to_raise


def check_task_state(task, exp_status, epx_done, exp_failed, exp_log_keys):
    assert task.status == exp_status
    assert task.done == epx_done
    assert task.failed == exp_failed
    assert set(task.log.keys()) == set(exp_log_keys)


def test_init_state():
    task = Task('foo_task', foo, (1,), {'b': 2, 'c': 3})
    check_task_state(task, task.PENDING, False, False, [task.PENDING])
    assert task.result is _undef_


def test_simple_task_pass():
    task = Task('foo_task', foo, (1,), {'b': 2, 'c': 3})
    task.execute()
    check_task_state(task, task.DONE, True, False, [task.PENDING, task.IN_PROGRESS, task.DONE])
    assert task.result == 6


def test_simple_task_fail():
    e = Exception('test')
    task = Task('foo_task', foo_raise, (e,), {})
    task.execute()
    check_task_state(task, task.DONE, True, True, [task.PENDING, task.IN_PROGRESS, task.DONE])
    assert task.result is e


def test_unexpected_args():
    task = Task('foo_task', foo, (1,), {'bb': 2, 'cc': 3})
    task.execute()
    check_task_state(task, task.DONE, True, True, [task.PENDING, task.IN_PROGRESS, task.DONE])
    assert isinstance(task.result, TypeError)
