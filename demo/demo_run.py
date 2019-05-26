import time

from schdlr.repeated import to_repeat
from schdlr.scheduler import Scheduler
from schdlr.task import FuncTask
from schdlr.workflow import Workflow


def printer(s):
    time.sleep(s)
    print("hello from %s" % s)


def another_printer(s):
    time.sleep(s)
    print("another hello from %s" % s)


tasks_produced = 0


def get_printer_tasks(num):
    global tasks_produced
    tasks = [FuncTask(printer, (i,), {})
             for i in range(tasks_produced, tasks_produced+num)]
    tasks_produced += num
    return tasks


def main():
    schdlr = Scheduler(2)

    t1, t2, t3, t4, t5 = get_printer_tasks(5)
    deps1 = {t1: [], t2: [t1], t3: [t2], t4: [t3], t5: [t4]}
    wf1 = Workflow(deps1, name="1st workflow")

    t6, t7, t8, t9 = get_printer_tasks(4)
    deps2 = {t7: [t6], t8: [t6], t9: [t6]}
    wf2 = Workflow(deps2, name="2nd workflow")

    schdlr.start()

    schdlr.add_workflow(wf1)
    schdlr.add_workflow(wf2)

    time.sleep(30)

    t10, t11, t12, t13 = get_printer_tasks(4)
    deps3 = {t10: [], t11: [t10], t12: [t11], t13: [t12]}
    wf3 = Workflow(deps3, name='3rd workflow')

    repeated_task = to_repeat(FuncTask(another_printer, (5, ), {}), 10)
    schdlr.add_repeated(repeated_task)

    schdlr.add_workflow(wf3)
    time.sleep(60)

    schdlr.stop()


if __name__ == "__main__":
    main()
