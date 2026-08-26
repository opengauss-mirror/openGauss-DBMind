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

"""
Description: Divide logs by workflow and template
"""

from collections import defaultdict

from .log_template_miner import LogTemplateMiner
from ..consts.constants import KERNEL_CONTENT_PATTERNS


class Workflow:
    """
    Description: Divide logs by workflow and template
    """
    def __init__(self, templates=None):
        self.templates = templates
        self.wf_dict = defaultdict(list)
        self.wf_regex_dict = defaultdict(list)
        self.last_updated_time = 0

    def add(self, logs, model):
        """to manage log lines"""
        n_logs = len(logs)
        for i, log in enumerate(logs):
            log_time = log.get("timestamp")
            if log_time < self.last_updated_time:
                continue

            self.last_updated_time = log_time

            content = log.get("content")
            if not content:
                continue

            if not log.get("kernel"):
                self.wf_regex_dict[content].append(log)
                continue

            if any(pattern.search(content) for pattern in KERNEL_CONTENT_PATTERNS):
                self.wf_regex_dict[content].append(log)

            log_level = log.get("level")
            if isinstance(log_level, str) and log_level.upper() in ["ERROR", "PANIC", "FATAL"]:
                self.wf_dict[log_level.lower()].append(log)
                if log_level.upper() in ["PANIC", "FATAL"]:
                    self.wf_dict["error"].append(log)

            if not isinstance(model, LogTemplateMiner):
                continue

            log_templates, _ = model.predict([content])
            log.update(log_templates[0])
            template = log.get("template")
            has_template = bool(isinstance(self.templates, dict) and self.templates)
            offset = self.templates.get(template, 0) if has_template else 0
            if offset and 0 <= i + offset <= n_logs - 1:
                log["content"] = logs[i + offset].get("content")

            self.wf_dict[template].append(log)

    def clear_queue(self):
        """to clear log lines"""
        self.wf_dict = defaultdict(list)
        self.wf_regex_dict = defaultdict(list)
