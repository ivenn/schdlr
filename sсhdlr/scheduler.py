import time
from queue import Queue, Empty
from threading import Thread, Event

from sсhdlr.task import Task
from .worker import Worker
from .log import get_logger


log = get_logger(__name__)


class Scheduler:

    STATE_NOT_STARTED = "NOT_STARTED"

    STATE_RUNNING = "RUNNING"

    STATE_STOPPING = "STOPPING"
    STATE_STOPPED = "STOPPED"

    def __init__(self, workers_num):
        self.workers_num = workers_num
        self.workers = []
        self._queue = Queue()  # TODO: should be a priority queue
        self._assigned_tasks = []
        self.main_t = None
        self.monitor_t = None  # TODO: should provide web interface
        self.task_producers = []    # TODO: for periodic tasks
        self._terminated = False
        self._accept_new_tasks = True
        self._empty_event = Event()
        self.state = self.STATE_NOT_STARTED

    def __repr__(self):
        return "Scheduler(queue={queue}, state={state})".format(
            queue=self._queue.qsize(),
            state=self.state
        )

    def add_task_group(self, task_group):
        if self._accept_new_tasks:
            self.producers.append(task_group)
        else:
            log.info("No new tasks!")

    def add_task(self, task):
        if self.state == self.STATE_NOT_STARTED:
            log.info("{task} was added to queue, but scheduler is not started".format(
                task=task
            ))
        elif self.state == self.STATE_STOPPING:
            log.ingo("{task} will not be added, scheduler is stopping".format(
                task=task
            ))
        else:
            self._queue.put(task)
            log.info("{task} was added to queue".format(
                task=task
            ))

    def run(self):
        self.workers = [Worker("work-%s" % i) for i in range(self.workers_num)]
        for w in self.workers:
            w.start()

        self.main_t = Thread(target=self._loop, name='schdlr')
        self.main_t.start()

        self.monitor_t = Thread(target=self._monitor, name='monitor')
        self.monitor_t.daemon = True
        self.monitor_t.start()

        self.state = self.STATE_RUNNING

    @property
    def workers_stat(self):
        return self.workers

    @property
    def tasks_stat(self):
        return self._assigned_tasks

    @property
    def stat(self):
        return self

    def _monitor(self):
        while True:
            log.info(self.stat)
            log.info(self.tasks_stat)
            log.info(self.workers_stat)
            time.sleep(1)

    def _loop(self):
        while True and not self._terminated:
            try:
                task = self._queue.get(timeout=1)
                task.status = Task.SCHEDULED
            except Empty:
                self._empty_event.set()
                continue
            self._empty_event.clear()
            worker = None
            while not worker:
                workers_ready = [w for w in self.workers if w.ready]
                if workers_ready:
                    worker = workers_ready[0]
            log.info("Assigning %s to %s" % (task, worker.name))
            self._assigned_tasks.append(task)
            worker.do(task)
        for w in self.workers:
            w.stop(wait=True)

    def stop(self,):
        if self.state in [self.STATE_RUNNING, ]:
            log.info("Stopping scheduler...")
            self.state = self.STATE_STOPPING
            self._empty_event.wait()
            self._terminated = True
            self.main_t.join()
            self.state = self.STATE_STOPPED
        else:
            log.warn("Scheduler is not started or already stopping/stopped!")




