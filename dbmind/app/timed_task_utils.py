# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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

"""Timed task multi-process executable function.
"""
import json

from dbmind.components.cluster_diagnosis import cluster_diagnosis, utils


def detect_anomaly(detector, long_term_metrics):
    """The anomaly detection timed task executable function."""
    return detector.detect(long_term_metrics)


def diagnose_cluster_state(instance, role, start_datetime, end_datetime, method):
    """The cluster diagnosis timed task executable function."""
    features, status_code = cluster_diagnosis.cluster_diagnose(
        instance=instance,
        role=role,
        start_time=start_datetime,
        end_time=end_datetime,
        method=method
    )
    result = utils.ANSWER_ORDERS[role].get(status_code, "Unknown")
    features = json.dumps(features)
    diagnose_result = {
        'instance': instance,
        'role': role,
        'timestamp': int(end_datetime.timestamp() * 1000),
        'method': method,
        'feature': features,
        'result': result,
        'status_code': status_code,
        'alarm_type': utils.TYPE_AND_LEVEL.get(role, {}).get(result, {}).get("alarm_type"),
        'alarm_level': utils.TYPE_AND_LEVEL.get(role, {}).get(result, {}).get("alarm_level")
    }
    return diagnose_result
