# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import logging
from threading import Thread, Event

from dbmind import global_vars, constants

minimal_timed_task_interval = 30


class RepeatedTimer(Thread):
    """RepeatedTimer class.
    This class inherits from `threading.Thread`,
     which triggers a periodic func at a fixed interval.
    """

    def __init__(self, interval, function, *args, **kwargs):
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._finished = Event()
        Thread.__init__(self, daemon=True)

    def run(self):
        while not self._finished.is_set():
            try:
                self._function(*self._args, **self._kwargs)
            except Exception as e:
                logging.error('RepeatedTimer<%s%s, %d> occurred an error because %s.'
                              % (self._function.__name__, self._args, self._interval, e))
                logging.exception(e)
            self._finished.wait(self._interval)
        self._finished.set()

    def cancel(self):
        self._finished.set()

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, seconds):
        self._interval = seconds

    def __hash__(self):
        return hash((self._interval, self._function))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._interval == other._interval and self._function == other._function
        else:
            return False

    def __str__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self._function.__name__, self._interval)

    def __repr__(self):
        return self.__str__()


class _TimedTaskManager:
    def __init__(self):
        self.task_table = dict()
        self.timers = dict()
        self.specified_timed_task = []

    def apply(self, func, seconds):
        if func.__name__ not in self.timers:
            self.timers[func.__name__] = RepeatedTimer(seconds, func)
            logging.info('Applied timed-task %s.', func.__name__)
        else:
            logging.info("The timed-task %s has been already started.", func.__name__)
        self.task_table[func] = seconds

    def start(self, timed_task=None):
        if timed_task is not None:
            timed_tasks = {timed_task: self.timers.get(timed_task)}
        else:
            timed_tasks = self.timers
        for timed_task, t in timed_tasks.items():
            # the task has been applied but not started
            t.start()
            logging.info("Start timed-task '%s'.", timed_task)

    def stop(self, timed_task=None):
        if timed_task is not None:
            timed_tasks = {timed_task: self.timers.get(timed_task)}
        else:
            timed_tasks = self.timers.copy()
        for timed_task, t in timed_tasks.items():
            # first stop the task
            t.cancel()
            # remove from timer list
            self.timers.pop(timed_task)
            logging.info("The timed-task '%s' has been stopped.", timed_task)

    def flush(self):
        # flush the timed_task which including:
        #  1). stop illegal timed-task.
        #  2). start the user-specified timed-task
        illegal_timed_task = set(self.timers.keys()) - set(self.specified_timed_task)
        for timed_task in illegal_timed_task:
            self.stop(timed_task)
        for timed_task in self.specified_timed_task:
            if timed_task not in global_vars.timed_task:
                logging.error("Timed-task '%s' not existed.", timed_task)
            else:
                func = global_vars.timed_task[timed_task]['object']
                if timed_task in (constants.DAILY_INSPECTION,
                                  constants.WEEKLY_INSPECTION, constants.MONTHLY_INSPECTION):
                    seconds = global_vars.timed_task[timed_task].get('seconds')
                else:
                    seconds = global_vars.configs.getint('TIMED_TASK', f'{timed_task}_interval',
                                                         fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
                if not self.check(timed_task):
                    self.apply(func, seconds)
                    self.start(timed_task)
                else:
                    self.reset_interval(timed_task, seconds)

    def check(self, timed_task=None):
        # check if task has been started
        return True if timed_task in self.timers else False

    def is_alive(self, timed_task):
        # determine whether the timed-task is alive
        if timed_task not in self.timers:
            return False
        t = self.timers[timed_task]
        return t.is_alive()

    def reset_interval(self, timed_task, seconds):
        # avoid task blocking caused by user interval setting too small, currently supported minimum interval is 30s
        seconds = minimal_timed_task_interval if seconds < minimal_timed_task_interval else seconds
        if self.check(timed_task):
            t = self.timers.get(timed_task)
            t.interval = seconds
            # update running interval
            global_vars.timed_task[timed_task]['seconds'] = seconds

    def get_interval(self, timed_task):
        t = self.timers.get(timed_task)
        return t.interval

    def run_once(self):
        for f in self.task_table:
            logging.info('Running timed task: %s.', f.__name__)
            try:
                f()
            except Exception as e:
                logging.exception(e)


TimedTaskManager = _TimedTaskManager()


def timer(seconds):
    """DBMind built-in timer."""
    def inner(func):
        TimedTaskManager.apply(func, seconds)
        return func

    return inner


def customized_timer(seconds):
    """User customized timer."""
    def inner(func):
        global_vars.timed_task[func.__name__] = {'object': func, 'seconds': seconds}
        if func.__name__ in global_vars.default_timed_task:
            TimedTaskManager.apply(func, seconds)
        return func

    return inner
