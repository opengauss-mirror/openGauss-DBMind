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

import argparse
import os
import sys
from datetime import datetime, timedelta

from prettytable import PrettyTable

from dbmind import constants
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.utils.component import initialize_tsdb_param
from dbmind.common.utils.checking import date_type, path_type, CheckIP, check_ip_valid
from dbmind.common.utils.exporter import set_logger
from dbmind.components.cluster_diagnosis.instance_feature import get_instance_features
from dbmind.components.cluster_diagnosis.fault_locating import cn_diagnosis, dn_diagnosis
from dbmind.components.cluster_diagnosis.xgb_predict import cn_xgb_diagnosis, dn_xgb_diagnosis
from dbmind.components.cluster_diagnosis.utils import (
    CM_ERROR,
    CN_STATUS,
    DN_STATUS,
    CN_ANSWER,
    DN_ANSWER,
    STATUS_MAP,
    ANSWER_ORDERS,
    ANSWER_MAP
)

WINDOW_IN_MINUTES = 3

MODELS = {
    "logical": {
        "cn": cn_diagnosis,
        "dn": dn_diagnosis
    },
    "tree": {
        "cn": cn_xgb_diagnosis,
        "dn": dn_xgb_diagnosis
    }
}

ROLE_STATUS = {
    "cn": CN_STATUS,
    "dn": DN_STATUS
}

ROLE_ANSWERS = {
    "cn": CN_ANSWER,
    "dn": DN_ANSWER,
}


def params_check(instance, role, method):
    if not check_ip_valid(instance):
        raise ValueError(f"Illegal IP: {instance}")

    if method not in tuple(MODELS.keys()):
        raise ValueError(
            f"argument 'method': invalid choice: '{method}' "
            f"(choose from {tuple(MODELS.keys())})"
        )

    if role not in tuple(MODELS["logical"].keys()):
        raise ValueError(
            f"argument 'role': invalid choice: '{role}' "
            f"(choose from {tuple(MODELS['logical'].keys())})"
        )


def cluster_diagnose(instance, role, start_time, end_time, method="logical"):
    params_check(instance, role, method)
    features = get_instance_features(instance, role, start_time, end_time)

    other_features = [value for name, value in features.items() if name != role + "_status"]
    status_list, offline = features.get(role + "_status", ([], False))

    if offline or (not any(other_features) and not any(status_list)):
        features[role + "_status"] = 0
        return features, -1

    model = MODELS.get(method).get(role)
    status_list = features.get(role + "_status")[0].copy()
    result_list = list()
    for status in status_list:
        if status == ROLE_STATUS[role][CM_ERROR]:
            result_list.append(ROLE_ANSWERS[role][CM_ERROR])
            continue

        features[role + "_status"] = status
        if method == "tree":
            result_list.append(model(features)[0])
        else:
            result_list.append(model(features))

    last_status = float("-inf")
    max_idx = 0
    max_res = max(result_list)
    for i, res in enumerate(result_list):
        status = status_list[i]
        if res == max_res and status > last_status:
            last_status = status
            max_idx = i

    features[role + "_status"] = status_list[max_idx]
    status_code = result_list[max_idx] if any(features.values()) else -1

    return features, int(status_code)


def main(argv):
    parser = argparse.ArgumentParser(description="Cluster diagnosis.")
    parser.add_argument('--conf', required=True, type=path_type,
                        help='set the directory of configuration files')
    parser.add_argument('--host', required=True, action=CheckIP,
                        help='set the host of the cluster node, ip only.')
    parser.add_argument('--role', required=True, choices=tuple(MODELS["logical"].keys()),
                        help='set the role of instance for diagnosis. '
                             'roles: [cn] are not supported for centralized DB.')
    parser.add_argument('--time', type=date_type,
                        help='set time for diagnosis in timestamp(ms) or datetime format')
    parser.add_argument('--method', default="logical", choices=tuple(MODELS.keys()),
                        help='set method for the model: logical: if-else, tree: xgboost.')

    args = parser.parse_args(argv)

    instance = args.host
    role = args.role
    method = args.method
    conf_path = args.conf
    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.\n' % args.conf)
    os.chdir(conf_path)
    set_logger(os.path.join('logs', constants.CLUSTER_DIAGNOSIS_LOG_NAME), "info")
    init_global_configs(conf_path)

    end_ts_in_ms = args.time
    if end_ts_in_ms is None:
        end_ts_in_ms = int(datetime.now().timestamp()) * 1000

    end_datetime = datetime.fromtimestamp(end_ts_in_ms / 1000)
    start_datetime = end_datetime - timedelta(minutes=WINDOW_IN_MINUTES)

    if not initialize_tsdb_param():
        parser.exit(1, "TSDB service does not exist, exiting...\n")

    features, status_code = cluster_diagnose(
        instance,
        role,
        start_datetime,
        end_datetime,
        method=method,
    )

    pt = PrettyTable()
    pt.field_names = ('Item', 'Result')
    for item, result in features.items():
        if item in STATUS_MAP:
            pt.add_row((ANSWER_MAP.get(item, item), STATUS_MAP[item].get(result)))
        else:
            pt.add_row((ANSWER_MAP.get(item, item), "Bad" if result else "Good"))

    result = ANSWER_ORDERS[role].get(status_code, "Unknown")

    pt.add_row(("Output", result))
    print(pt)


if __name__ == '__main__':
    main(sys.argv[1:])
