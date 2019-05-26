import subprocess
import time

from schdlr.task import Task


class FuncTask(Task):

    def __init__(self, func, args, kwargs, timeout=None):
        super(FuncTask, self).__init__(func.__name__, timeout)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def copy(self):
        return FuncTask(self.func, self.args, self.kwargs)

    def executable(self):
        return self.func(*self.args, **self.kwargs)


class SleepTask(Task):

    def __init__(self, to_sleep):
        super(SleepTask, self).__init__("sleep", timeout=to_sleep+1)
        self.to_sleep = to_sleep

    def copy(self):
        return SleepTask(self.to_sleep)

    def executable(self):
        return time.sleep(self.to_sleep)


class ShellTask(Task):

    def __init__(self, cmd, timeout=60):
        super(ShellTask, self).__init__("shell", timeout=timeout+1)
        self.cmd = cmd
        self.timeout = timeout

    def copy(self):
        return SleepTask(self.cmd)

    def executable(self):
        proc = subprocess.Popen(self.cmd, shell=True,
                                stdout=subprocess.PIPE,)
        out, err = proc.communicate(timeout=self.timeout)
        retcode = proc.returncode
        if retcode:
            raise subprocess.CalledProcessError(retcode, self.cmd)
        else:
            return out




