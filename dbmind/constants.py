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
PIDFILE_NAME = 'dbmind.pid'
LOGFILE_NAME = 'dbmind.log'
SLOW_SQL_RCA_LOG_NAME = 'slow_sql_diagnosis.log'
MEMORY_CHECKER_LOG_NAME = 'memory_checker.log'
METRIC_MAP_CONFIG = 'metric_map.conf'
METRIC_VALUE_RANGE_CONFIG = "metric_value_range.conf"
MUST_FILTER_LABEL_CONFIG = 'filter_label.conf'
DYNAMIC_CONFIG = 'dynamic_config.db'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

with open(os.path.join(MISC_PATH, VERFILE_NAME)) as fp:
    __version__ = fp.readline().strip()

DBMIND_UI_DIRECTORY = os.path.join(DBMIND_PATH, '../ui/build')

# The default timed-task when the service is not started with only-run.
# User should not stop the 'DISCARD_EXPIRED_RESULTS' to avoid excessive disk usage.
DISCARD_EXPIRED_RESULTS = 'discard_expired_results'
ANOMALY_DETECTION_NAME = 'anomaly_detection'
DAILY_INSPECTION = 'daily_inspection'
WEEKLY_INSPECTION = 'weekly_inspection'
MONTHLY_INSPECTION = 'monthly_inspection'
INDEX_RECOMMEND = 'index_recommend'
KNOB_RECOMMEND = 'knob_recommend'
SLOW_SQL_DIAGNOSIS = 'slow_sql_diagnosis'
SLOW_QUERY_KILLER = 'slow_query_killer'
# If the user does not provide a task run interval, the following default values will be used.
TIMED_TASK_DEFAULT_INTERVAL = 24 * 60 * 60
DEFAULT_TASK_NAMES = (ANOMALY_DETECTION_NAME, DISCARD_EXPIRED_RESULTS,
                      DAILY_INSPECTION, WEEKLY_INSPECTION, MONTHLY_INSPECTION)
OPTIONAL_TASK_NAMES = (INDEX_RECOMMEND, KNOB_RECOMMEND, SLOW_QUERY_KILLER, SLOW_SQL_DIAGNOSIS)
# Notice: 'DISTINGUISHING_INSTANCE_LABEL' is a magic string, i.e., our own name.
# Thus, not all collection agents (such as Prometheus's openGauss-exporter)
# distinguish different instance addresses through this one.
# Actually, this is a risky action for us, currently.
DISTINGUISHING_INSTANCE_LABEL = 'from_instance'
EXPORTER_INSTANCE_LABEL = 'instance'
