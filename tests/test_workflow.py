from schdlr import workflow
from schdlr import task


class DummyTask:

    def __init__(self, name):
        self.name = name
        self._status = task.PENDING

    def __repr__(self):
        return "Task({name}, {status})".format(
            name=self.name, status=self._status)

    @property
    def failed(self):
        return self._status == task.FAILED

    @property
    def done(self):
        return self._status == task.DONE

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        self._status = new_status


def check_wf_state(wf, exp_state,
                   exp_to_do=None, exp_in_progress=None, exp_done=None):
    assert wf.state == exp_state, "{act} != {exp}".format(act=wf.state, exp=exp_state)
    if exp_to_do:
        assert set(wf.tasks_to_do) == set(exp_to_do), "{act} != {exp}".format(
            act=wf.tasks_to_do, exp=exp_to_do)
    if exp_in_progress:
        assert set(wf.tasks_in_progress) == set(exp_in_progress), "{act} != {exp}".format(
            act=wf.tasks_in_progress, exp=exp_in_progress)
    if exp_done:
        assert set(wf.tasks_done) == set(exp_done), "{act} != {exp}".format(
            act=wf.tasks_done, exp=exp_done)


def test_chain_workflow():
    """
    t1 -> t2 -> t3 -> ...
    """
    t1, t2, t3 = [DummyTask('foo_%s' % i) for i in range(1, 4)]
    deps = {t1: [], t2: [t1], t3: [t2]}

    wf = workflow.Workflow(deps)
    check_wf_state(wf, workflow.NOT_STARTED)

    to_do = wf.to_do()
    assert to_do == [t1, ], "{act} != {exp}".format(act=to_do, exp=[t1, ])
    check_wf_state(wf, workflow.NOT_STARTED,
                   exp_to_do=[t1, ], exp_in_progress=[], exp_done=[])

    # check that nothing changes and t_do returns the same tasks
    to_do = wf.to_do()
    assert to_do == [t1, ], "{act} != {exp}".format(act=to_do, exp=[t1, ])
    check_wf_state(wf, workflow.NOT_STARTED,
                   exp_to_do=[t1, ], exp_in_progress=[], exp_done=[])

    t1.status = task.SCHEDULED
    to_do = wf.to_do()
    assert to_do == [], "{act} != {exp}".format(act=to_do, exp=[])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[], exp_in_progress=[t1, ], exp_done=[])

    t1.status = task.IN_PROGRESS
    to_do = wf.to_do()
    assert to_do == [], "{act} != {exp}".format(act=to_do, exp=[])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[], exp_in_progress=[t1, ], exp_done=[])

    t1.status = task.DONE
    to_do = wf.to_do()
    assert to_do == [t2, ], "{act} != {exp}".format(act=to_do, exp=[t2, ])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[t2, ], exp_in_progress=[], exp_done=[t1, ])

    t2.status = task.DONE
    to_do = wf.to_do()
    assert to_do == [t3, ], "{act} != {exp}".format(act=to_do, exp=[t3, ])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[t3, ], exp_in_progress=[], exp_done=[t1, t2, ])

    t3.status = task.DONE
    to_do = wf.to_do()
    assert to_do is None, "{act} != {exp}".format(act=to_do, exp=None)
    check_wf_state(wf, workflow.COMPLETE,
                   exp_to_do=None, exp_in_progress=[], exp_done=[t1, t2, t3])


def test_parallel_tasks_workflow():
    """
          t2
        /    \
    t1 -> t3 -> t5
        \    /
          t4
    """
    t1, t2, t3, t4, t5 = [DummyTask('foo_%s' % i) for i in range(1, 6)]
    deps = {t1: [], t2: [t1], t3: [t1], t4: [t1], t5: [t2, t3, t4]}

    wf = workflow.Workflow(deps)

    to_do = wf.to_do()
    assert to_do == [t1, ], "{act} != {exp}".format(act=to_do, exp=[t1, ])
    check_wf_state(wf, workflow.NOT_STARTED,
                   exp_to_do=[t1, ], exp_in_progress=[], exp_done=[])

    t1.status = task.DONE
    to_do = wf.to_do()
    assert set(to_do) == set([t2, t3, t4]), "{act} != {exp}".format(act=to_do, exp=[t2, t3, t4])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[t2, t3, t4], exp_in_progress=[], exp_done=[t1, ])

    t2.status = task.IN_PROGRESS
    t3.status = task.IN_PROGRESS
    t4.status = task.IN_PROGRESS
    to_do = wf.to_do()
    assert to_do == [], "{act} != {exp}".format(act=to_do, exp=[])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[], exp_in_progress=[t2, t3, t4], exp_done=[t1, ])

    t2.status = task.DONE
    to_do = wf.to_do()
    assert to_do == [], "{act} != {exp}".format(act=to_do, exp=[])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[], exp_in_progress=[t3, t4], exp_done=[t1, t2])

    t3.status = task.DONE
    t4.status = task.DONE
    to_do = wf.to_do()
    assert to_do == [t5, ], "{act} != {exp}".format(act=to_do, exp=[t5, ])
    check_wf_state(wf, workflow.IN_PROGRESS,
                   exp_to_do=[t5, ], exp_in_progress=[], exp_done=[t1, t2, t3, t4])

    t5.status = task.DONE
    to_do = wf.to_do()
    assert to_do is None, "{act} != {exp}".format(act=to_do, exp=None)
    check_wf_state(wf, workflow.COMPLETE,
                   exp_to_do=None, exp_in_progress=[], exp_done=[t1, t2, t3, t4, t5])

    def test_complex_workflow():
        """
             t5 -> t6
            /        \
        t1 -> t2 ---> t3
            \        /
             t4 -----
         t7
           \
           t8
          /
        t9
        """
        t1, t2, t3, t4, t5, t6, t7, t8, t9 = [
            DummyTask('foo_%s' % i) for i in range(1, 10)]

        deps = {
            # t1: [],
            t2: [t1],
            t3: [t2, t4, t6],
            t4: [t1],
            t5: [t1],
            t6: [t5],
            # t7: [],
            t8: [t7, t9],
            # t9: [],
        }




