import os
import signal
import time
from queue import Empty, PriorityQueue
from threading import Thread, Event, RLock

from schdlr import task
from schdlr import etc
from schdlr import worker
from schdlr import workflow


logger = etc.get_logger(__name__)


class ExitCommand(Exception):
    pass


def scheduler_force_stop(signal, frame):
    raise ExitCommand("Stopping Scheduler because of workers blocked by overdue tasks")


signal.signal(signal.SIGUSR1, scheduler_force_stop)


STATE_NOT_STARTED = "NOT_STARTED"
STATE_RUNNING = "RUNNING"
STATE_STOPPING = "STOPPING"
STATE_STOPPED = "STOPPED"


class Scheduler:

    def __init__(self, workers_num, monitor=False, max_blocked_workers_ratio=1):
        self.workers_num = workers_num
        self.workers = []
        self.max_blocked_workers_ratio = max_blocked_workers_ratio

        self._stat_lock = RLock()
        self._wf_lock = RLock()
        self._pqueue = PriorityQueue()
        self._tasks_in_progress = []
        self._tasks_done = []
        self._waiting_worker = None

        self.main_t = None
        self.monitor = monitor
        self.monitor_t = None  # TODO: should provide web interface

        self.workflows = []
        self.periodic = []

        self._terminated = False
        self._empty_event = Event()
        self._state = STATE_NOT_STARTED

    def __repr__(self):
        return "Scheduler(queue={queue}, state={state})".format(
            queue=self._pqueue.qsize(),
            state=self.state
        )

    def add_task(self, new_task, priority=None, timeout=None):
        if not isinstance(new_task, task.Task):
            return

        if self.state in [STATE_STOPPING, STATE_STOPPED]:
            logger.info("{task} will not be added, scheduler is stopping".format(
                task=new_task
            ))
        else:
            if priority:
                new_task.priority = priority
            if timeout:
                new_task.timeout = timeout

            new_task.state = task.SCHEDULED
            self._pqueue.put(new_task)

            if self.state == STATE_NOT_STARTED:
                logger.info("{task} was added to queue, but scheduler is not started".format(
                    task=new_task
                ))
            else:
                logger.info("{task} was added to queue".format(
                    task=new_task
                ))

    def add_workflow(self, wf):
        with self._wf_lock:
            self.workflows.append(wf)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        logger.info("{old} -> {new}".format(old=self._state, new=new_state))
        self._state = new_state

    def start(self):
        self.workers = [worker.Worker("work-%s" % i) for i in range(self.workers_num)]
        for w in self.workers:
            w.start()

        self.main_t = Thread(target=self._loop, name='schdlr')
        self.main_t.start()

        self.monitor_t = Thread(target=self._monitor, name='monitor')
        self.monitor_t.start()

        self.state = STATE_RUNNING

    @property
    def workers_stat(self):
        return self.workers

    def _recalculate_task_stat(self):
        with self._stat_lock:
            newly_done = []
            new_in_progress = []
            for t in self._tasks_in_progress:
                if t.done:
                    newly_done.append(t)
                else:
                    new_in_progress.append(t)

            self._tasks_in_progress = new_in_progress
            self._tasks_done += newly_done

    def tasks_stat(self, short=False):
        self._recalculate_task_stat()
        if short:
            task_stat = {
                'in_queue': len(list(self._pqueue.queue)),
                'in_progress': len(self._tasks_in_progress),
                'waiting_for_worker': self._waiting_worker,
                'done': len(self._tasks_done)}
        else:
            task_stat = {
                'in_queue': list(self._pqueue.queue),
                'in_progress': self._tasks_in_progress,
                'done': self._tasks_done}
        return task_stat

    @property
    def overdue_tasks(self):
        return [t for t in self._tasks_in_progress if t.timed_out]

    @property
    def stat(self):
        return self

    def _monitor(self):
        while True and not self._terminated:
            if self.monitor:
                logger.info(self.tasks_stat(short=True))
            if (len(self.overdue_tasks) / self.workers_num) >= self.max_blocked_workers_ratio:
                os.kill(os.getpid(), signal.SIGUSR1)
            time.sleep(1)

    def _loop(self):
        while not self._terminated:
            workflows = []
            for wf in self.workflows:
                tasks = wf.to_do()
                if tasks is None and wf.state == workflow.COMPLETED:
                    continue
                for t in tasks:
                    self.add_task(t)
                workflows.append(wf)
            with self._wf_lock:
                self.workflows = workflows

            try:
                tsk = self._pqueue.get(timeout=0.1)
                self._waiting_worker = tsk.name
            except Empty:
                self._empty_event.set()
                continue
            self._empty_event.clear()

            worker = None
            logger.info("%s is waiting worker" % tsk)
            while not worker and not self._terminated:
                workers_ready = [w for w in self.workers if w.ready]
                if workers_ready:
                    logger.info("Worker found: %s" % workers_ready)
                    worker = workers_ready[0]
                    self._waiting_worker = None

            logger.info("Assigning %s to %s" % (tsk, worker.name))
            worker.do(tsk)
            self._tasks_in_progress.append(tsk)

        for w in self.workers:
            w.stop(wait=True)

    def stop(self, complete_enqueued=False):
        if self.state in [STATE_RUNNING, ]:
            logger.info("Stopping scheduler...")
            self.state = STATE_STOPPING
            if not complete_enqueued:
                self._empty_event.wait()
            self._terminated = True
            self.main_t.join()
            self.monitor_t.join()
            self.state = STATE_STOPPED
        else:
            logger.warn("Scheduler is not started or already stopping/stopped!")







