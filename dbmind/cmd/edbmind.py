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
"""DBMind common functionality interface"""

import logging
import os
import signal
import sys
import threading
import time
import traceback

from dbmind import app
from dbmind import constants
from dbmind import controllers
from dbmind import global_vars
from dbmind.cmd.configs.config_utils import (
    load_sys_configs
)
from dbmind.cmd.configs.configurators import DynamicConfig
from dbmind.common.utils.base import try_to_get_an_element
from dbmind.common import platform
from dbmind.common import utils
from dbmind.common.daemon import Daemon, read_dbmind_pid_file
from dbmind.common.dispatcher import TimedTaskManager
from dbmind.common.dispatcher import get_worker_instance
from dbmind.common.exceptions import SetupError
from dbmind.common.http import HttpService
from dbmind.common.tsdb import TsdbClientFactory

# Support input() function in using backspace.
try:
    import readline
except ImportError:
    pass

_http_service = HttpService()
dbmind_master_should_exit = False


def _check_confpath(confpath):
    confile_path = os.path.join(confpath, constants.CONFILE_NAME)
    if os.path.exists(confile_path):
        return True
    return False


def _process_clean(force=False):
    # Wait for workers starting.
    while global_vars.worker is None:
        time.sleep(.1)
    global_vars.worker.terminate(cancel_futures=force)
    TimedTaskManager.stop()
    _http_service.shutdown()


def signal_handler(signum, frame):
    global dbmind_master_should_exit

    if signum == signal.SIGINT or signum == signal.SIGHUP:
        utils.cli.write_to_terminal('Reloading parameters.', color='green')
        global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
        specified_timed_tasks = global_vars.configs.get('TIMED_TASK', 'TASK')
        TimedTaskManager.specified_timed_task = utils.split(specified_timed_tasks)
        if constants.DISCARD_EXPIRED_RESULTS not in TimedTaskManager.specified_timed_task:
            # 'DISCARD_EXPIRED_RESULTS' does not support user modification. Therefore,
            # if it is found that the task does not exist in the specified_timed_tasks,
            # it will be forcibly added to global_vars.backend_timed_task.
            TimedTaskManager.specified_timed_task.append(constants.DISCARD_EXPIRED_RESULTS)
        # refresh timed-tasks
        TimedTaskManager.flush()
    elif signum == signal.SIGUSR2:
        # used for debugging
        with open('traceback.stack', 'w+') as f:
            for th in threading.enumerate():
                f.write(str(th) + '\n')
                current_frames = getattr(sys, '_current_frames')()
                traceback.print_stack(current_frames[th.ident], file=f)
                f.write('\n')
    elif signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logging.info('DBMind received exit signal.')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean()
    elif signum == signal.SIGQUIT:
        # Force to exit and cancel future tasks.
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        logging.info('DBMind received the signal: exit immediately.')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean(force=True)


def init_rpc_with_config(tsdb=None):
    master_url = utils.split(global_vars.configs.get('AGENT', 'master_url'))
    ssl_certfile = utils.split(global_vars.configs.get('AGENT', 'ssl_certfile'))
    ssl_keyfile = utils.split(global_vars.configs.get('AGENT', 'ssl_keyfile'))
    ssl_keyfile_password = utils.split(global_vars.configs.get('AGENT', 'ssl_keyfile_password'))
    ssl_ca_file = utils.split(global_vars.configs.get('AGENT', 'ssl_ca_file'))
    agent_username = utils.split(global_vars.configs.get('AGENT', 'username'))
    agent_pwd = utils.split(global_vars.configs.get('AGENT', 'password'))

    # -- check for agent configurations below --
    def is_the_same_length(*args):
        idx = 1
        while idx < len(args):
            if len(args[idx]) != len(args[idx - 1]):
                return False
            idx += 1
        return True

    error_msg = ("The agent ssl configuration is incorrect, "
                 "keep the number of setting elements of "
                 "these configurations the same.")
    if not is_the_same_length(
            ssl_certfile, ssl_keyfile, ssl_keyfile_password,
            ssl_ca_file
    ):
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )
    if not is_the_same_length(
            agent_username, agent_pwd
    ):
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )
    # If master url doesn't set, we don't need to do the
    # subsequent checks.
    if len(master_url) > 0 and not is_the_same_length(
            master_url, ssl_certfile
    ) and len(ssl_certfile) > 1:
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )
    if len(master_url) > 0 and not is_the_same_length(
            master_url, agent_username
    ) and len(ssl_certfile) > 1:
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )

    # -- finish checking --
    # If user doesn't set any urls, we can
    # try to scan and discover the urls by TSDB metrics.
    # But auto-discover has a restriction, which is
    # user should set a username. Otherwise, we can't
    # connect to the agent with any credentials.
    # Meanwhile, the username should be one element.
    if tsdb and len(master_url) == 0:
        try:
            if (
                len(agent_username) != 1 or
                len(ssl_certfile) > 1
            ):
                utils.raise_fatal_and_exit(
                    "You haven't set master_url, which means "
                    "you employ autodiscover mode. You only can "
                    "set ONE same username/password/SSL for all cluster.",
                    use_logging=False
                )
            global_vars.agent_proxy.set_autodiscover_connection_info(
                username=try_to_get_an_element(agent_username, 0),
                password=try_to_get_an_element(agent_pwd, 0),
                ssl_certfile=try_to_get_an_element(ssl_certfile, 0),
                ssl_keyfile=try_to_get_an_element(ssl_keyfile, 0),
                ssl_key_password=try_to_get_an_element(ssl_keyfile_password, 0),
                ca_file=try_to_get_an_element(ssl_ca_file, 0)
            )
            global_vars.agent_proxy.autodiscover(tsdb)
        except Exception as e:
            logging.warning(
                'Cannot extract agent url from TSDB.', exc_info=e
            )

    for i, url in enumerate(master_url):
        global_vars.agent_proxy.agent_add(
            url=url,
            username=try_to_get_an_element(agent_username, i),
            password=try_to_get_an_element(agent_pwd, i),
            ssl_certfile=try_to_get_an_element(ssl_certfile, i),
            ssl_keyfile=try_to_get_an_element(ssl_keyfile, i),
            ssl_key_password=try_to_get_an_element(ssl_keyfile_password, i),
            ca_file=try_to_get_an_element(ssl_ca_file, i)
        )

    global_vars.agent_proxy.agent_finalize()
    return global_vars.agent_proxy


def init_logger_with_config():
    log_directory = global_vars.configs.get('LOG', 'log_directory', fallback='logs')
    log_directory = os.path.realpath(log_directory)
    os.makedirs(log_directory, exist_ok=True)
    max_bytes = global_vars.configs.getint('LOG', 'maxbytes')
    backup_count = global_vars.configs.getint('LOG', 'backupcount')
    logging_handler = utils.MultiProcessingRFHandler(filename=os.path.join(log_directory, constants.LOGFILE_NAME),
                                                     maxBytes=max_bytes,
                                                     backupCount=backup_count)
    logging_handler.setFormatter(
        logging.Formatter("[%(asctime)s %(levelname)s][%(process)d-%(thread)d][%(name)s]: %(message)s")
    )
    logger = logging.getLogger()
    logger.name = 'DBMind'
    logger.addHandler(logging_handler)
    logger.setLevel(global_vars.configs.get('LOG', 'level').upper())
    return logging_handler


def init_global_configs(confpath):
    global_vars.confpath = confpath
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    global_vars.dynamic_configs = DynamicConfig()
    global_vars.metric_map.update(
        utils.read_simple_config_file(
            constants.METRIC_MAP_CONFIG
        )
    )
    global_vars.metric_value_range_map.update(
        utils.read_simple_config_file(constants.METRIC_VALUE_RANGE_CONFIG)
    )
    global_vars.must_filter_labels = utils.read_simple_config_file(
        constants.MUST_FILTER_LABEL_CONFIG
    )


def init_tsdb_with_config():
    # Set the information for TSDB.
    TsdbClientFactory.set_client_info(
        global_vars.configs.get('TSDB', 'name'),
        global_vars.configs.get('TSDB', 'host'),
        global_vars.configs.get('TSDB', 'port'),
        global_vars.configs.get('TSDB', 'username'),
        global_vars.configs.get('TSDB', 'password'),
        global_vars.configs.get('TSDB', 'ssl_certfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile_password'),
        global_vars.configs.get('TSDB', 'ssl_ca_file')
    )
    return TsdbClientFactory.get_tsdb_client()


class DBMindMain(Daemon):
    def __init__(self, confpath):
        if not _check_confpath(confpath):
            raise SetupError("Invalid directory '%s', please set up first." % confpath)

        self.confpath = os.path.realpath(confpath)
        self.worker = None

        pid_file = os.path.join(confpath, constants.PIDFILE_NAME)
        super().__init__(pid_file)

    def run(self):
        os.chdir(self.confpath)
        os.umask(0o0077)

        mismatching_message = (
            "The version of the configuration file directory "
            "does not match the DBMind, "
            "please use the '... setup --initialize' command to reinitialize."
        )
        if not os.path.exists(constants.VERFILE_NAME):
            utils.raise_fatal_and_exit(
                mismatching_message, use_logging=False
            )
        with open(constants.VERFILE_NAME) as fp:
            if fp.readline().strip() != constants.__version__:
                utils.raise_fatal_and_exit(
                    mismatching_message, use_logging=False
                )

        utils.cli.set_proc_title('DBMind [Master Process]')
        # Set global variables.
        init_global_configs(self.confpath)

        # Set logger.
        logging_handler = init_logger_with_config()

        logging.info('DBMind is starting.')

        # Warn user of proxies if user set.
        if os.getenv('http_proxy') or os.getenv('https_proxy'):
            logging.warning('You set the proxy environment variable (e.g., http_proxy, https_proxy),'
                            ' which may cause network requests to be sent '
                            'through the proxy server, which may cause some network connectivity issues too.'
                            ' You need to make sure that the action is what you expect.')

        # Register signal handler.
        if not platform.WIN32:
            signal.signal(signal.SIGHUP, signal_handler)
            signal.signal(signal.SIGUSR2, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Initialize TSDB.
        tsdb = init_tsdb_with_config()

        # Initialize RPC agent.
        init_rpc_with_config(tsdb)
        for p in utils.split(global_vars.configs.get('AGENT', 'password')):
            logging_handler.add_sensitive_word(p)

        # Create executor pool.
        # Notice: we have to initialize the process pool
        # as early as possible because sub-processes were
        # forked from the main process, meanwhile copying
        # all states. These states may include timed tasks,
        # HTTP response buffers,  and other strange variables.
        # This copying behavior can cause many unexpected
        # and hard-to-debug errors. For example, the
        # sub-process raises 'cannot parse JSON' while getting
        # an HTTP response and parsing it, which is caused
        # the sub-process to copy a partial HTTP buffer.
        local_workers = global_vars.configs.getint('WORKER', 'process_num', fallback=-1)
        global_vars.worker = self.worker = get_worker_instance('local', local_workers)

        # Start timed tasks.
        app.register_timed_app()
        if global_vars.is_dry_run_mode:
            TimedTaskManager.run_once()
            return

        TimedTaskManager.start()
        # Start to create a web service.
        web_service_host = global_vars.configs.get('WEB-SERVICE', 'host')
        web_service_port = global_vars.configs.getint('WEB-SERVICE', 'port')
        ssl_certfile = global_vars.configs.get('WEB-SERVICE', 'ssl_certfile')
        ssl_keyfile = global_vars.configs.get('WEB-SERVICE', 'ssl_keyfile')
        ssl_keyfile_password = global_vars.configs.get('WEB-SERVICE', 'ssl_keyfile_password')
        ssl_ca_file = global_vars.configs.get('WEB-SERVICE', 'ssl_ca_file')

        # Attach rules for web service.
        for c in controllers.get_dbmind_controller():
            _http_service.register_controller_module(c)
        _http_service.mount_static_files(constants.DBMIND_UI_DIRECTORY)

        _http_service.start_listen(
            web_service_host,
            web_service_port,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_keyfile_password=ssl_keyfile_password,
            ssl_ca_file=ssl_ca_file
        )

        # Main thread will block here under Python 3.7.
        while not dbmind_master_should_exit:
            time.sleep(1)
        logging.info('DBMind will close.')

    def clean(self):
        pass

    def reload(self):
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            if platform.WIN32:
                os.kill(pid, signal.SIGINT)
            else:
                os.kill(pid, signal.SIGHUP)
        else:
            utils.cli.write_to_terminal(
                'Invalid DBMind process.',
                level='error',
                color='red'
            )
            os.remove(self.pid_file)
