from .log import get_logger

_logger = get_logger(__name__)

class Workflow:
    """
    Class provide ability to create tasks workflow 
    """

    STATE_NOT_STARTED = "NOT_STARTED"
    STATE_RUNNING = "RUNNING"
    STATE_STOPPED = "STOPPED"

    def __init__(self, name):
        self.name = name
        self._tasks_queue = []
        self._results = []
        self._error = None
        self._status = self.STATE_NOT_STARTED
        self._last_task = None

    def __repr__(self):
        return "Workflow(name='{name}', status={status}, last_task={last_task}, tasks_queue={tasks_queue}, resutlts={resutlts})".format(
            name = self.name,
            status=self._status,
            last_task = self._last_task,
            tasks_queue=self._tasks_queue,
            resutlts=self._results
        )

    def add_task(self, task):
        self._tasks_queue.append(task)

    def execute(self):
        _logger.info("Workflow(%s) execution is started", self.name)
        try:
            self._status = self.STATE_RUNNING
            for task in self._tasks_queue[:]:
                self._last_task = task
                task.execute()
                self._results.append(task.result)
                if task.failed:
                    self._error = task.result
                    _logger.error("%s in workflow %s falied with error %s", self._last_task, self, self._error)
                    break
        finally:
            self._status = self.STATE_STOPPED
            _logger.info("Workflow(%s) execution is finished", self.name)

    @property
    def last_result(self):
        return self._results[-1] if self._results else None
    
    @property
    def resutlts(self):
        return self._results
    
    @property
    def last_task(self):
        return self._last_task
