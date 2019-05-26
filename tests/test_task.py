from schdlr import task as task_lib
from schdlr import etc


class SumTask(task_lib.Task):

    def __init__(self, a=1, b=1, c=0, to_raise=None, timeout=None):
        super(SumTask, self).__init__("SumTask", timeout=timeout)
        self.a = a
        self.b = b
        self.c = c
        self.to_raise = to_raise

    def executable(self):
        if self.to_raise:
            raise self.to_raise
        else:
            all_sum = self.a + self.b + self.c
            if all_sum != 0:
                return all_sum


def check_task_state(t, exp_state, epx_done, exp_failed, exp_log_keys):
    assert t.state == exp_state
    assert t.done == epx_done
    assert t.failed == exp_failed
    assert set(t.events.keys()) == set(exp_log_keys)


def test_init_state():
    task = SumTask(a=1, b=2, c=3)
    check_task_state(task, task_lib.PENDING, False, False, [task_lib.PENDING])
    assert task.result is etc._undef_


def test_simple_task_pass():
    task = SumTask(a=1, b=2, c=3)
    task.execute()
    check_task_state(task, task_lib.DONE, True, False, [task_lib.PENDING, task_lib.IN_PROGRESS, task_lib.DONE])
    assert task.result == 6


def test_simple_task_pass_no_return():
    task = SumTask(a=0, b=0, c=0)
    task.execute()
    check_task_state(task, task_lib.DONE, True, False, [task_lib.PENDING, task_lib.IN_PROGRESS, task_lib.DONE])
    assert task.result is None


def test_simple_task_fail():
    exc_to_raise = Exception('test')
    task = SumTask(to_raise=exc_to_raise)
    task.execute()
    check_task_state(task, task_lib.FAILED, False, True, [task_lib.PENDING, task_lib.IN_PROGRESS, task_lib.FAILED])
    assert isinstance(task.traceback, str) and 'raise self.to_raise' in task.traceback
    assert task.result is exc_to_raise

