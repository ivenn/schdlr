from schdlr import task
from schdlr import etc


def foo(a, b=1, c=0):
    return a + b + c


def no_return():
    pass


def foo_raise(to_raise):
    raise to_raise


def check_task_state(t, exp_state, epx_done, exp_failed, exp_log_keys):
    assert t.state == exp_state
    assert t.done == epx_done
    assert t.failed == exp_failed
    assert set(t.events.keys()) == set(exp_log_keys)


def test_init_state():
    t = task.Task('foo_task', foo, (1,), {'b': 2, 'c': 3})
    check_task_state(t, task.PENDING, False, False, [task.PENDING])
    assert t.result is etc._undef_


def test_simple_task_pass():
    t = task.Task('foo_task', foo, (1,), {'b': 2, 'c': 3})
    t.execute()
    check_task_state(t, task.DONE, True, False, [task.PENDING, task.IN_PROGRESS, task.DONE])
    assert t.result == 6


def test_simple_task_pass_no_return():
    t = task.Task('no_return', no_return, (), {})
    t.execute()
    check_task_state(t, task.DONE, True, False, [task.PENDING, task.IN_PROGRESS, task.DONE])
    assert t.result is None


def test_simple_task_fail():
    e = Exception('test')
    t = task.Task('foo_task', foo_raise, (e,), {})
    t.execute()
    check_task_state(t, task.FAILED, False, True, [task.PENDING, task.IN_PROGRESS, task.FAILED])
    assert isinstance(t.traceback, str) and 'in foo_raise' in t.traceback
    assert t.result is e


def test_unexpected_args():
    t = task.Task('foo_task', foo, (1,), {'bb': 2, 'cc': 3})
    t.execute()
    check_task_state(t, task.FAILED, False, True, [task.PENDING, task.IN_PROGRESS, task.FAILED])
    assert isinstance(t.result, TypeError)
