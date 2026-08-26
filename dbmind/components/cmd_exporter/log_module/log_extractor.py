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

import json
import logging
import os
import time
from collections import defaultdict
from queue import Empty

from .log_agent.inotify import IN_MODIFY
from .log_agent.taildir_source import TaildirSource, FileMatcher
from .log_miner.consts.constants import (
    AUTO_SAVE_PERIOD,
    AUTO_TAIL_PERIOD,
    BLACKLIST,
    CONTENT_PATTERNS,
    CORE_DUMP,
    DB_VERSION,
    DEBUG_ID_PATTERN,
    DICTIONARY,
    KERNELS,
    LOGFORMAT_PATTERNS,
    METRICS_INFO,
    MODEL_SAVE_PATH,
    SCRAPE_WINDOW,
    SYSTEM_ALARM,
    TRIE_CONTENT_TRAIN,
    UNIQUE_SQL_PATTERN,
    WHITELIST,
)
from .log_miner.config_gs import log_template_config, log_formats, log_labels
from .log_miner.models.log_process import line_process, find_template
from .log_miner.models.log_template_miner import LogTemplateMiner
from .log_miner.models.workflow import Workflow
from ..cmd_module.impl import CACHE
from ..query import CumulativeMetric, Metric, QueryInterface

# To preserve the value of cumulative metrics between calls
global_cumulative_metrics_values_dict = dict()

_global_labels = {}
_system_alarms = defaultdict(dict)


def set_constant_labels(labels):
    _global_labels.update(labels)


def label_gen(content, name, labels_in_content):
    res = dict()
    if not labels_in_content or name not in CONTENT_PATTERNS:
        return res

    pattern = CONTENT_PATTERNS.get(name)
    patch = {labels_in_content[i]: v
             for i, v in enumerate(pattern.search(content).groups())}
    if None in patch:
        patch.pop(None)

    res.update(patch)
    return res


def init_miner(log_path):
    path, _ = os.path.split(log_path)
    last_model = os.path.join(MODEL_SAVE_PATH, path.replace("/", "_") + ".json")

    if os.path.isfile(last_model):
        model = LogTemplateMiner(verbosity=0)
        try:
            model.load_model_json(file_path=last_model)
            model.train_model(TRIE_CONTENT_TRAIN)
            model.save_model_json(file_path=last_model)
            os.chmod(last_model, 0o600)
            return model
        except json.decoder.JSONDecodeError:
            logging.warning("model json file: %s may be broken.", last_model)

    model = LogTemplateMiner(DICTIONARY, log_template_config)
    model.train_model(TRIE_CONTENT_TRAIN)
    model.save_model_json(file_path=last_model)
    os.chmod(last_model, 0o600)
    return model


def extract_ffic_log(log_path):
    labels = dict()
    try:
        with open(log_path, "r") as ffic_log:
            for line in ffic_log:
                if line == "====== Statememt info ======\n":
                    unique_sql = ffic_log.readline()
                    unique_sql_search = UNIQUE_SQL_PATTERN.search(unique_sql)
                    if unique_sql_search is not None:
                        labels["unique_sql_id"] = unique_sql_search.groups()[0]

                    debug_sql = ffic_log.readline()
                    debug_sql_search = DEBUG_ID_PATTERN.search(debug_sql)
                    if debug_sql_search is not None:
                        labels["debug_sql_id"] = debug_sql_search.groups()[0]

                    break

    except FileNotFoundError:
        logging.warning("ffic log is not found.")

    return labels


class LogExtractor(QueryInterface):
    def __init__(self, log_dir):
        self._log_dir = log_dir
        self.scrape_window = SCRAPE_WINDOW
        self.auto_save_period = AUTO_SAVE_PERIOD
        self.auto_tail_period = AUTO_TAIL_PERIOD
        self.delay = 0

        self.source = TaildirSource(
            matcher=FileMatcher(list()),
            watch_paths=[self._log_dir],
            blacklist=BLACKLIST,
            whitelist=WHITELIST
        )
        self.chan = self.source.channel
        self.source.start()
        self.last_tail_time = time.monotonic()
        self.successfully_returned = False

        self.miner_models = defaultdict(dict)
        self.last_save_time = time.monotonic()

        self.workflows = defaultdict(dict)

        # Parse dict structure to a Metric object
        self.metrics = list()
        for prefix, metrics in METRICS_INFO.items():
            for metric in metrics:
                attr = metric.copy()
                if attr["status"] == "disable":
                    continue

                name = attr.pop("name")
                attr["labels"] = sorted(list(set(attr["labels"]) | _global_labels.keys()))
                attr["template"] = find_template(attr.get("template"), DB_VERSION)
                if attr.get("type") in ["trie", "level", "cumulative"]:
                    attr["template"] = attr.get("template", "").lower()

                single_metric = Metric(name, attr)
                single_metric.set_prefix(prefix)
                self.metrics.append(single_metric)

        self.templates = defaultdict(dict)
        for metric in self.metrics:
            template = metric.template
            offset = metric.offset
            for miner_type in metric.miner_type.split(","):
                if miner_type in KERNELS:
                    self.templates[miner_type][template] = offset

        self.system_alarm_metrics = {
            metric.type: metric for metric in self.metrics
            if metric.miner_type == "system_alarm"
        }

    def update(self):
        # Clear the value and label of the old metric.
        if self.successfully_returned:
            for metric in self.metrics:
                if metric.usage != "COUNTER":
                    metric.entity.clear()

        self.successfully_returned = False

        chroot_prefix = CACHE.get("opengauss_xlog", {}).get("chroot_prefix", "")
        if chroot_prefix and chroot_prefix != self.source.chroot_prefix:
            logging.info("Taildir's watcher restarted due to chroot_prefix: %s.", chroot_prefix)
            self.source.chroot_prefix = chroot_prefix
            self.source.restart_watcher()

        if not self.chan.qsize():
            logging.warning("Tail dir source fetched nothing.")
            self.check_taildir_source()
            return

        updated, updated_set = list(), set()
        for _ in range(self.chan.qsize()):
            try:
                event = self.chan.get_nowait()
            except Empty:
                logging.warning("Tail dir source fetched nothing.")
                self.check_taildir_source()
                break

            log_path = event.header.get("path")
            mask = event.header.get("type")
            body = event.body
            if (log_path, body) not in updated_set:
                updated.append((log_path, body, mask))
                updated_set.add((log_path, body))

        # query new log blocks
        self.delay = 0
        block_list = set()  # block the postgresql log with full log-statement
        log_dict, log_contents = defaultdict(dict), defaultdict(dict)
        for log_path, body, mask in updated:
            self.last_tail_time = time.monotonic()
            path, filename = os.path.split(log_path)
            if path in block_list:
                continue

            for miner_type in log_formats:  # To determine the miner type
                if filename.startswith(miner_type):
                    break
            else:
                continue

            if miner_type in KERNELS and path not in self.miner_models[miner_type]:
                self.miner_models[miner_type][path] = init_miner(log_path)
            elif miner_type in CORE_DUMP:
                if mask not in [IN_MODIFY]:
                    continue

                for metric in self.metrics:
                    if miner_type in metric.miner_type.split(",") and metric.template == "core":
                        ffic_labels = extract_ffic_log(log_path)
                        update_labels = {label_name: ffic_labels.get(label_name, "")
                                         for label_name in metric.labels}
                        update_labels.update(_global_labels)
                        metric.entity.labels(**update_labels).set(1)
                        break

                continue

            elif miner_type in SYSTEM_ALARM:
                if not body:
                    body = self.source.tail_files.get(log_path).read()

                try:
                    self.update_system_alarms(body)
                except Exception as e:
                    logging.exception(e)
                    break

                continue

            if path not in self.workflows[miner_type]:  # save results into the workflows
                self.workflows[miner_type][path] = Workflow(self.templates.get(miner_type))

            if path not in log_dict[miner_type]:
                log_dict[miner_type][path] = list()

            if path not in log_contents[miner_type]:
                log_contents[miner_type][path] = list()

            if not body:
                body = self.source.tail_files.get(log_path).read()

            headers, log_pattern = LOGFORMAT_PATTERNS[miner_type]
            lines, lines_set = body.split("\n"), set()
            for line in lines:
                if line in lines_set:
                    continue

                lines_set.add(line)
                message = line_process(line, log_pattern, headers)
                if message and isinstance(message, dict):
                    content = message.get("content")
                    if (
                        miner_type in KERNELS and
                        content.startswith(
                            ("statement:",
                             "parameters:",
                             "[Current Statement]",
                             "Bypass execute")
                        )
                    ):  # bypass when GUC parameter: log_statement = "all"
                        log_dict[miner_type].pop(path)
                        log_contents[miner_type].pop(path)
                        block_list.add(path)
                        break

                    message["kernel"] = miner_type in KERNELS
                    log_dict[miner_type][path].append(message)
                    log_contents[miner_type][path].append(content)
                # To coherent multiple log lines.
                elif (
                    miner_type in KERNELS and
                    log_dict[miner_type][path] and
                    (not line.startswith(time.strftime("%Y-%m", time.localtime(time.time()))))
                ):
                    log_dict[miner_type][path][-1]["content"] += line.replace("\t", " ")
                    log_contents[miner_type][path][-1] += line.replace("\t", " ")

        if block_list:
            logging.warning("The GUC parameter 'log_statement' was changed to 'all'. "
                            "These pg logs' extraction was blocked: %s.", block_list)

        for miner_type, path_logs in log_dict.items():
            for path, logs in path_logs.items():
                model = self.miner_models[miner_type].get(path, None)  # load model
                self.workflows[miner_type][path].add(logs, model)
                workflow = self.workflows[miner_type][path]
                self.query_metrics_from_workflow(workflow, miner_type)  # set metric values
                self.workflows[miner_type][path].clear_queue()  # clear the workflows

        self.update_resident_system_alarm()

        if self.delay > 0:
            logging.info("The log delay has been up to %s seconds.", self.delay)

        # save models periodically
        if time.monotonic() - self.last_save_time >= self.auto_save_period:
            self.save_models()
            self.last_save_time += self.auto_save_period

    def query_metrics_from_workflow(self, workflow, miner_type):
        label_names = log_labels[miner_type].copy()
        the_query_moment = time.time()
        for metric in self.metrics:
            if miner_type not in metric.miner_type.split(","):
                continue

            logs = list()
            if metric.type in ["regex"]:
                for content in workflow.wf_regex_dict:
                    log_list = workflow.wf_regex_dict.get(content)
                    pattern = CONTENT_PATTERNS.get(metric.name)
                    if log_list and pattern.search(content) is not None:
                        logs.extend(log_list)

            elif metric.type in ["trie", "level", "cumulative"]:
                template = metric.template
                logs = workflow.wf_dict.get(template, [])

            metric_dict = self.generate_labels(logs, metric, the_query_moment, label_names)
            self.update_metrics(metric, metric_dict)

    def generate_labels(self, logs, metric, the_query_moment, label_names):
        """ Append correct values to each metric label values by metric type"""
        metric_dict = defaultdict(int)
        for i, log in enumerate(logs):
            delay = the_query_moment - log["timestamp"] / 1000
            if delay > self.scrape_window:
                self.delay = max(delay, self.delay)
                continue

            labels = {v: log.get(v) for v in label_names}
            content_labels = label_gen(log.get("content"), metric.name, metric.labels_in_content)
            labels = dict(labels, **content_labels)
            metric_labels = {label: labels.get(label) for label in metric.labels}
            label_key = tuple(sorted(metric_labels.items()))
            metric_dict[label_key] += 1

        return metric_dict

    @staticmethod
    def update_metrics(metric: Metric, metric_dict: dict):
        """  Set the correct values for each metric by its labels """
        global global_cumulative_metrics_values_dict
        for label_key, value in metric_dict.items():
            metric_labels = {k: v for k, v in label_key}
            metric_labels.update(_global_labels)
            if metric.name == "recycle_lsn" and metric_labels.get("recycle_lsn"):
                recycle_lsn = metric_labels["recycle_lsn"]
                value = int(recycle_lsn[-16: -8] + recycle_lsn[-2:], 16)
                metric.entity.labels(**metric_labels).set(value)
                continue

            if metric.type != "cumulative":
                metric.entity.labels(**metric_labels).set(value)
            else:
                # get the current value
                cumulative_metric = global_cumulative_metrics_values_dict.get(
                    metric.name, CumulativeMetric(metric.name)
                )
                cumulative_metric.set(value=value, labels=metric_labels)
                metric.entity.labels(**metric_labels).set(cumulative_metric.get(metric_labels))
                # store back the new value the current value
                global_cumulative_metrics_values_dict[metric.name] = cumulative_metric

    def update_system_alarms(self, body):
        for line in body.split("\n"):
            try:
                content_dict = json.loads(line)
            except json.decoder.JSONDecodeError:
                continue

            alarm_name = content_dict.get("name")
            if alarm_name not in self.system_alarm_metrics:
                continue

            if alarm_name == "AbnormalTopologyConnect":
                self.abnormal_topology_connect(content_dict)

    def abnormal_topology_connect(self, content_dict):
        start_timestamp = int(content_dict.get("start_timestamp"))
        end_timestamp = int(content_dict.get("end_timestamp"))
        timestamp = max(start_timestamp, end_timestamp)
        delay = time.time() - timestamp / 1000
        if delay > self.scrape_window:
            return

        metric = self.system_alarm_metrics["AbnormalTopologyConnect"]
        pattern = CONTENT_PATTERNS.get(metric.name)
        search_result = pattern.search(content_dict.get("source_tag", ""))
        if search_result is None:  # not cn or dn
            return

        tag = search_result.groups()
        labels = {"role": tag[0], "instance_id": tag[1], "node_code": tag[2]}
        update_labels = {label_name: labels.get(label_name, "") for label_name in metric.labels}
        update_labels.update(_global_labels)
        label_values = (tag[0], tag[1], tag[2])
        if content_dict.get("op_type") == "firing":
            _system_alarms["AbnormalTopologyConnect"][label_values] = update_labels
        elif content_dict.get("op_type") == "resolved":
            if label_values in _system_alarms["AbnormalTopologyConnect"]:
                _system_alarms["AbnormalTopologyConnect"].pop(label_values)

    def update_resident_system_alarm(self):
        for alarm_name, metric_info in _system_alarms.items():
            for update_labels in metric_info.values():
                metric = self.system_alarm_metrics[alarm_name]
                metric.entity.labels(**update_labels).set(1)

    def attach(self, registry):
        for metric in self.metrics:
            metric.activate(metric.labels, _global_labels)
            registry.register(metric.entity)

    def save_models(self):
        for miner_type, path_model in self.miner_models.items():
            for path, model in path_model.items():
                model_json = os.path.join(MODEL_SAVE_PATH, path.replace("/", "_") + ".json")
                model.save_model_json(file_path=model_json)
                os.chmod(model_json, 0o600)

    def check_taildir_source(self):
        # restart the tailing of log directory for a long time without scraping.
        if time.monotonic() - self.last_tail_time >= self.auto_tail_period:
            logging.info("Taildir's watcher restarted: %s.", self._log_dir)
            self.source.restart_watcher()
            self.last_tail_time = time.monotonic()

    def close(self):
        self.source.stop()
