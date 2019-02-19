from .task import Task


def foo(a, b=1, res=None):
    if isinstance(res, Exception):
        raise res
    else:
        return a, b, res


def test_simple():
    t = Task('foo_task', foo, (1,), {'b': 2, 'res': 3})
    print(t.status, t.result, t.done, t.failed)

    t.execute()

    print(t.status, t.result, t.done, t.failed)


def test_simple_fail():
    t = Task('foo_task', foo, (1,), {'b': 2, 'res': Exception('test')})
    print(t.status, t.result, t.done, t.failed)

    t.execute()

    print(t.status, t.result, t.done, t.failed)