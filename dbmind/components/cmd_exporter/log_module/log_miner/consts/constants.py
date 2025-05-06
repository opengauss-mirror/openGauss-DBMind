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

import json
import re
import os
import shutil

import yaml

from dbmind.components.cmd_exporter.cmd_module.utils import perform_shell_command

from ..config_gs import log_formats
from ..models.log_process import compile_logformat_pattern, find_template

PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DICTIONARY_PATH = os.path.join(PATH, "dictionary/validword_dict_default.json")
WORDNINJA_PATH = os.path.join(PATH, "dictionary/wordninja.json")
MODEL_SAVE_PATH = os.path.join(PATH, 'model_save')

if os.path.exists(MODEL_SAVE_PATH):
    shutil.rmtree(MODEL_SAVE_PATH)
os.mkdir(MODEL_SAVE_PATH)
os.chmod(MODEL_SAVE_PATH, 0o700)

SCRAPE_WINDOW = 360
AUTO_SAVE_PERIOD = 600
AUTO_TAIL_PERIOD = 300

WHITELIST = ["/pg_log/cn", "/gs_log/cn", "/pg_log/dn", "/gs_log/dn",
             "/pg_log/gtm", "/gs_log/gtm", "/cm_agent", "/cm_server",
             "/ffic_log"]
BLACKLIST = ["/cm_agent/pg_log", "/cm_agent/gs_log", "/cm_agent/system_stat",
             "/cm_server/key_event", "/cm_server/system_alarm", "/gtm/system_alarm",
             "/cm_agent/gsmonitor"]

KERNELS = {"postgresql", "opengauss"}
MODULES = {"cm_agent", "cm_server", "system_call", "gtm"}
CORE_DUMP = {"ffic_opengauss"}
SYSTEM_ALARM = {"system_alarm"}

EXITCODE, GSQL_V = perform_shell_command("gsql -V", stdin="", timeout=1)
DB_VERSION = "else"

LOGFORMAT_PATTERNS = {miner_type: compile_logformat_pattern(log_format)
                      for miner_type, log_format in log_formats.items()}

with open(DICTIONARY_PATH, "r") as f:
    DICTIONARY = json.load(f)

COMP_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..")
YAML_PATH = os.path.join(COMP_PATH, "yamls", "log_metrics.yml")
with open(YAML_PATH, errors="ignore") as fp:
    METRICS_INFO = yaml.safe_load(fp)

TRIE_CONTENT_TRAIN = list()
KERNEL_CONTENT_PATTERNS = list()
CONTENT_PATTERNS = dict()
for _, log_metrics in METRICS_INFO.items():
    for log_metric in log_metrics:
        if log_metric["status"] == "disable":
            continue

        log_template = find_template(log_metric.get("template"), DB_VERSION)
        if log_metric["type"] == "trie":
            new_template = log_template.replace("[", r"\[").replace("(", r"\(").replace(")", r"\)")
            re_pattern = re.compile(new_template.replace("<*>", r"([\S]+)"))
        else:
            re_pattern = re.compile(log_template.replace("<*>", r"([\S]+)"))

        CONTENT_PATTERNS[log_metric.get("name")] = re_pattern
        if not set(log_metric["miner_type"].split(",")) & KERNELS:
            continue

        if log_metric["type"] == "regex":
            KERNEL_CONTENT_PATTERNS.append(re_pattern)
        elif log_metric["type"] == "trie":
            TRIE_CONTENT_TRAIN.append(log_template.lower())

UNIQUE_SQL_PATTERN = re.compile(r"^\[statement] unique SQL key - sql id: (.*?), cn id: .*?, user id: .*?\n$")
DEBUG_ID_PATTERN = re.compile(r"^\[statement] debug query id: (.*?)\n$")
