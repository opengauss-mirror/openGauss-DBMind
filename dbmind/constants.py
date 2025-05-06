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
import os

__description__ = 'openGauss DBMind: An autonomous platform for openGauss'

DBMIND_PATH = os.path.dirname(os.path.realpath(__file__))
MISC_PATH = os.path.join(DBMIND_PATH, 'misc')

VERFILE_NAME = 'VERSION'
CONFILE_NAME = 'dbmind.conf'  # the name of configuration file
PIDFILE_NAME = 'dbmind.pid'  # the name of pid file
CMD_PIDFILE_NAME = 'cmd_exporter.pid'
REPROCESSING_PIDFILE_NAME = 'reprocessing_exporter.pid'
LOGFILE_NAME = 'dbmind.log'
SLOW_QUERY_RCA_LOG_NAME = 'slow_query_diagnosis.log'
MEMORY_CHECKER_LOG_NAME = 'memory_checker.log'
ANOMALY_DETECTION_LOG_NAME = 'anomaly_detection.log'
FETCH_STATEMENT_LOG_NAME = 'fetch_statement.log'
CLUSTER_DIAGNOSIS_LOG_NAME = 'cluster_diagnosis.log'
METRIC_MAP_CONFIG = 'metric_map.conf'
METRIC_VALUE_RANGE_CONFIG = "metric_value_range.conf"
MUST_FILTER_LABEL_CONFIG = 'filter_label.conf'
DYNAMIC_CONFIG = 'dynamic_config.db'
SCENARIO_YAML_FILE_NAME = "security_scenarios.yml"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
PORT_SUFFIX = "(:[0-9]{4,5})"
CIPHER_S1 = 'encryption_part_a.bin'

with open(os.path.join(MISC_PATH, VERFILE_NAME)) as fp:
    __version__ = fp.readline().strip()

DBMIND_UI_DIRECTORY = None

# The default timed-task when the service is not started with only-run.
# User should not stop the 'DISCARD_EXPIRED_RESULTS' to avoid excessive disk usage.
DISCARD_EXPIRED_RESULTS = 'discard_expired_results'
ANOMALY_DETECTION_NAME = 'anomaly_detection'
CLUSTER_DIAGNOSIS_NAME = 'cluster_diagnose'
KNOB_RECOMMEND = 'knob_recommend'
SLOW_SQL_DIAGNOSIS = 'slow_sql_diagnosis'
SLOW_QUERY_DIAGNOSIS = 'slow_query_diagnosis'
SLOW_QUERY_KILLER = 'slow_query_killer'
AGENT_UPDATE_DETECT = 'agent_update_detect'
CALIBRATE_SECURITY_METRICS = 'calibrate_security_metrics'
CHECK_SECURITY_METRICS = 'check_security_metrics'
UPDATE_STATISTICS = 'update_statistics'
# If the user does not provide a task run interval, the following default values will be used.
TIMED_TASK_DEFAULT_INTERVAL = 24 * 60 * 60
MINIMAL_TIMED_TASK_INTERVAL = 30
TASK_NAMES = (DISCARD_EXPIRED_RESULTS, ANOMALY_DETECTION_NAME, CLUSTER_DIAGNOSIS_NAME, AGENT_UPDATE_DETECT,
              UPDATE_STATISTICS, KNOB_RECOMMEND, SLOW_QUERY_KILLER, SLOW_QUERY_DIAGNOSIS,
              CALIBRATE_SECURITY_METRICS, CHECK_SECURITY_METRICS)
# Notice: 'DISTINGUISHING_INSTANCE_LABEL' is a magic string, i.e., our own name.
# Thus, not all collection agents (such as Prometheus's openGauss-exporter)
# distinguish different instance addresses through this one.
# Actually, this is a risky action for us, currently.
DISTINGUISHING_INSTANCE_LABEL = 'from_instance'
EXPORTER_INSTANCE_LABEL = 'instance'
