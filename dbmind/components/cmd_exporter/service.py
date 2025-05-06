# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from types import SimpleNamespace

from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind import constants
from dbmind.common import ha
from dbmind.common.utils import dbmind_assert

from . import cli
from . import cmd_module
from . import log_module

FROM_INSTANCE_KEY = 'from_instance'
ROUND_THRESHOLD = 15
_thread_pool_executor = SimpleNamespace()
_registry = CollectorRegistry()

lock = threading.Lock()
query_instances = dict()
tasks = list()
task_conditions = defaultdict(bool)


def mark_task_as_finished(name):
    def callback(future):
        with lock:
            task_conditions[name] = False

    return callback


def config_collecting_params(parallel, constant_labels):
    global _thread_pool_executor

    _thread_pool_executor = ThreadPoolExecutor(max_workers=parallel)
    # Append extra labels, including essential labels (e.g., from_server)
    # and constant labels from user's configurations.
    cmd_module.set_constant_labels(constant_labels)
    log_module.set_constant_labels(constant_labels)
    logging.info('Perform shell commands with %d threads, extra labels: %s.',
                 parallel, constant_labels)


def register_metrics(parsed_yml, log_dir):
    # Construct command line query using the given parsed_yml.
    dbmind_assert(isinstance(parsed_yml, dict))
    # Construct log collector.
    for name, raw_query_instance in parsed_yml.items():
        dbmind_assert(isinstance(raw_query_instance, dict))
        cmd_query = cmd_module.CmdQuery(name, raw_query_instance)
        cmd_query.attach(_registry)
        query_instances[name] = cmd_query
        tasks.append(name)

    if log_dir:
        log_extractor = log_module.LogExtractor(log_dir)
        log_extractor.attach(_registry)
        query_instances["log_extractor"] = log_extractor
        tasks.insert(0, "log_extractor")


def query_all_metrics():
    start = time.monotonic()
    result = generate_latest(_registry)
    for name in tasks:
        if not task_conditions.get(name):
            future = _thread_pool_executor.submit(query_instances[name].update)
            with lock:
                task_conditions[name] = True

            callback_func = mark_task_as_finished(name)
            future.add_done_callback(callback_func)
        else:
            logging.warning(f"Task {name} is unfinished.")

    if time.monotonic() - start < ROUND_THRESHOLD:
        query_instances["log_extractor"].successfully_returned = True

    return result


def block_query_all_metrics():
    futures = []
    for name, query in query_instances.items():
        if not task_conditions.get(name):
            future = _thread_pool_executor.submit(query.update)
            task_conditions[name] = True
            callback_func = mark_task_as_finished(name)
            future.add_done_callback(callback_func)
            futures.append(future)
        else:
            logging.warning(f"Task {name} is unfinished.")

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logging.exception(e)

    return generate_latest(_registry)


def check_status_cmd_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = cli.exporter_info_dict['logfile']
    if cli.exporter_info_dict['constant_labels_instance']:
        pid_file = os.path.join(proj_path,
                                'cmd_exporter_{}.pid'.format(cli.exporter_info_dict['constant_labels_instance']))
    else:
        pid_file = os.path.join(proj_path, constants.CMD_PIDFILE_NAME)
    status_info = ha.check_status_impl(log_path, pid_file, 'cmd_exporter', ())
    ha.record_interface_info('check_status', status_info)
    return status_info


def repair_interface_cmd_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = cli.exporter_info_dict['logfile']
    if cli.exporter_info_dict['constant_labels_instance']:
        pid_file = os.path.join(proj_path,
                                'cmd_exporter_{}.pid'.format(cli.exporter_info_dict['constant_labels_instance']))
    else:
        pid_file = os.path.join(proj_path, constants.CMD_PIDFILE_NAME)
    repair_info = ha.repair_interface_impl(log_path, pid_file, 'cmd_exporter', ())
    ha.record_interface_info('repair', repair_info)
    return repair_info
