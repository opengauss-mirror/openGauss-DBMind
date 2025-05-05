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
import time
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np

from dbmind import global_vars
from dbmind.common.algorithm import anomaly_detection
from dbmind.common.algorithm.forecasting.forecasting_algorithm import quickly_forecast
from dbmind.common.types import Sequence
from dbmind.common.utils import adjust_timezone
from dbmind.common.utils.checking import prepare_ip, split_ip_port
from dbmind.components.cluster_diagnosis.cluster_diagnosis import WINDOW_IN_MINUTES
from dbmind.constants import PORT_SUFFIX
from dbmind.metadatabase import dao
from dbmind.service import dai
from dbmind.service.dai import is_sequence_valid
from dbmind.service.multicluster import get_remote_instance_addresses
from dbmind.service.web.context_manager import ACCESS_CONTEXT_NAME, get_access_context
from dbmind.service.web.jsonify_utils import (
    sqlalchemy_query_jsonify_for_multiple_instances,
    sqlalchemy_query_jsonify
)

from .monitoring_constants import (
    SIX_HOUR_IN_SECONDS,
    ONE_DAY_IN_SECONDS,
    ONE_WEEK_IN_SECONDS,
    ONE_MONTH_IN_SECONDS,
    DEFAULT_STEP,
    MIN_STEP,
    INCREASE_MIN_LENGTH,
    DEFAULT_FORECAST_TIME,
    DEFAULT_UPPER_THRESHOLD,
    DEFAULT_LOWER_THRESHOLD,
    DEFAULT_FORECAST_PARAM,
    DEFAULT_DOWNSAMPLE_LENGTH,
    LONG_TRANSACTION_DURATION,
    RECOMMEND_LOWER_RATE,
    RECOMMEND_UPPER_RATE,
    MAX_FORECAST_NUM,
    FULL_INSP_ITEM_LIST,
    COUPLE_INSP_ITEM_DICT,
    INCREASE_INSP_ITEM_LIST,
    THRESHOLD_INSP_ITEM_LIST,
    FORECAST_INSP_ITEM_LIST,
    FTYPE_INSP_ITEM_LIST,
    FIXED_INSP_ITEM_LIST,
    FULL_LOG_ERROR_LIST,
    WARNING_INFO_DICT,
    SCORE_WEIGHT_DICT,
    LONG_TRANSACTION_SQL,
    TOP_QUERIES_SQL
)

_continuous_increasing_detector = anomaly_detection.IncreaseDetector()
prefix_dict = {}
ip_dict = {}


class RecordSequence:
    def __init__(self, timestamps=None, values=None, ftype=False):
        if values is None:
            values = []

        if timestamps is None:
            timestamps = []

        self.timestamps = timestamps
        self.values = values
        self.ftype = ftype

    def extend_data(self, timestamps, values):
        if len(self.timestamps) == 0 or len(timestamps) <= 1:
            self.timestamps.extend(timestamps)
            self.values.extend(values)
        else:
            self.timestamps.extend(timestamps[1:])
            self.values.extend(values[1:])


def get_statistic_data(values):
    avg_val, max_val, min_val, the_95th_val = 0, 0, 0, 0
    if values:
        avg_val = sum(values) / len(values)
        max_val = max(values)
        min_val = min(values)
        the_95th_val = np.nanpercentile(values, 95)

    return avg_val, min_val, max_val, the_95th_val


def get_ip_map_dict():
    data_to_management_dict = {}
    management_to_data_dict = {}
    for data_ip, management_dict in global_vars.ip_map.items():
        for management_ip, management_info in management_dict.items():
            if management_info == data_ip:
                data_to_management_dict[data_ip] = management_ip
                management_to_data_dict[management_ip] = data_ip

    return data_to_management_dict, management_to_data_dict


def get_cluster_instances(instance, username, password):
    instance_list = []
    try:
        for agent, rpc_client in global_vars.agent_proxy:
            if agent != instance:
                continue

            instance_list.extend(get_remote_instance_addresses(rpc_client))

    except Exception:
        instance_list = []

    if not instance_list:
        instance_list = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
    if not instance_list:
        if instance not in global_vars.agent_proxy.agents:
            return instance_list

        with global_vars.agent_proxy.context(instance, username, password):
            instance_list = global_vars.agent_proxy.current_cluster_instances()
    return instance_list


def get_cluster_type_view(instance, username, password):
    if instance not in global_vars.agent_proxy.agents:
        return None
    with global_vars.agent_proxy.context(instance, username, password):
        stmt = "SELECT pg_catalog.gs_deployment() AS gs_deployment;"
        res = global_vars.agent_proxy.call('query_in_postgres', stmt)
        if len(res) != 1:
            return None
        if 'gs_deployment' not in res[0].keys():
            return None
        view_type = res[0]['gs_deployment']
        if 'Centralized' in view_type:
            return 'centralize'
        elif 'Distribute' in view_type:
            return 'distribute'
        else:
            return None


def get_instance_cluster_type(instance, username, password):
    cluster_type = get_cluster_type_view(instance, username, password)
    if cluster_type:
        return cluster_type

    return None


def get_instance_type_dict(instance, username, password):
    type_dict = {
        'C': set(),
        'D': set(),
        'S': set(),
    }
    type_list = type_dict.keys()
    instance_type_dict = {}
    with global_vars.agent_proxy.context(instance, username, password):
        stmt = "SELECT node_type, node_host, node_port FROM pg_catalog.pgxc_node;"
        res = global_vars.agent_proxy.call('query_in_postgres', stmt)
        for node_info in res:
            if node_info["node_type"] not in type_list:
                continue
            cur_instance = f"{prepare_ip(node_info['node_host'])}:{node_info['node_port']}"
            type_dict[node_info['node_type']].add(cur_instance)
            instance_type_dict[cur_instance] = node_info['node_type']
    return type_dict, instance_type_dict


def extend_instance_type(type_dict, instance_type_dict):
    cur_instance_list = list(instance_type_dict.keys())
    cur_ip_list = [split_ip_port(instance)[0] for instance in cur_instance_list]
    type_list = type_dict.keys()
    for ip, ip_list in ip_dict.items():
        if ip not in cur_ip_list:
            continue
        cur_instance = cur_instance_list[cur_ip_list.index(ip)]
        port = split_ip_port(cur_instance)[1]
        if not port:
            continue
        for tmp_ip in ip_list:
            tmp_instance = f"{prepare_ip(tmp_ip)}:{port}"
            instance_type_dict[tmp_instance] = instance_type_dict[cur_instance]
            if instance_type_dict[cur_instance] not in type_list:
                continue
            type_dict[instance_type_dict[cur_instance]].add(tmp_instance)


def get_instance_type_management(management_ip_list, instance_type_dict):
    ip_list = ip_dict.keys()
    instance_type_list = instance_type_dict.keys()
    coordinator_list = []
    datanode_list = []
    standby_list = []
    for management_instance in management_ip_list:
        management_ip, management_port = split_ip_port(management_instance)
        if management_ip not in ip_list:
            raise Exception("can not get instance type of ip: {}".format(management_ip))
        cur_instance_ips = ip_dict[management_ip]
        for cur_instance_ip in cur_instance_ips:
            cur_instance = f"{prepare_ip(cur_instance_ip)}:{management_port}"
            if cur_instance not in instance_type_list:
                continue
            cur_instance_type = instance_type_dict[cur_instance]
            if cur_instance_type == 'C':
                coordinator_list.append(cur_instance)
            elif cur_instance_type == 'D':
                datanode_list.append(cur_instance)
            elif cur_instance_type == 'S':
                standby_list.append(cur_instance)
    return coordinator_list, datanode_list, standby_list


def get_instance_list_management(instance, username, password):
    instance_list = get_cluster_instances(instance, username, password)
    management_ip_list = set()
    data_ip_list = set()
    data_to_management_dict, management_to_data_dict = get_ip_map_dict()
    global_data_ip_list = data_to_management_dict.keys()
    global_management_ip_list = management_to_data_dict.keys()
    # no ip_map
    if not (global_management_ip_list and global_data_ip_list):
        for instance in instance_list:
            instance_ip, instance_port = split_ip_port(instance)
            data_ip_list.add(f"{prepare_ip(instance_ip)}:{instance_port}")
            management_ip_list.add(f"{prepare_ip(instance_ip)}:{instance_port}")
        return management_ip_list, data_ip_list
    for instance in instance_list:
        instance_ip, instance_port = split_ip_port(instance)
        if instance_ip in global_data_ip_list:
            data_ip_list.add(f"{prepare_ip(instance_ip)}:{instance_port}")
            management_ip_list.add(f"{prepare_ip(data_to_management_dict[instance_ip])}:{instance_port}")
        elif instance_ip in global_management_ip_list:
            management_ip_list.add(f"{prepare_ip(instance_ip)}:{instance_port}")
            data_ip_list.add(f"{prepare_ip(management_to_data_dict[instance_ip])}:{instance_port}")
    return management_ip_list, data_ip_list


def get_cluster_diagnosis_records(instance, start_time, end_time, role):
    """
    get cluster diagnosis records from tb_history_cluster_diagnosis

    - param instance: instances without port
    - param start_time: filter results whose timestamp > start_time
    - param end_time: filter results whose timestamp < end_time
    """
    diagnosis_args = {
        "instance": instance,
        "cluster_role": role,
        "diagnosis_method": "logical",
        "start_at": int(datetime.timestamp(start_time) * 1000),
        "end_at": int(datetime.timestamp(end_time) * 1000),
        "is_normal": True
    }
    result = dao.cluster_diagnosis_records \
        .select_history_cluster_diagnosis(query_all=False, **diagnosis_args)
    field_names = result.statement.columns.keys()
    query_result = sqlalchemy_query_jsonify(result, field_names)
    timestamp_list = []
    value_list = []
    for item in query_result.get('rows'):
        timestamp_list.append(item[0])
        value_list.append(item[1])
    return timestamp_list, value_list


class MultipleHoursInspection:
    def __init__(self, instance, username, password, start=None, end=None, step=DEFAULT_STEP):
        self._report = {}
        self._username = username
        self._password = password
        self._start = start
        self._end = end
        self._step = step
        self._agent_instance = instance
        self._agent_instance_no_port = split_ip_port(self._agent_instance)[0]
        management_ip_list, data_ip_list = get_instance_list_management(self._agent_instance, username, password)
        self._instances_with_port = management_ip_list
        self._instances_with_no_port = set([split_ip_port(i)[0] for i in self._instances_with_port])
        self.data_ip_list = data_ip_list
        self.data_ip_with_no_port = set([split_ip_port(i)[0] for i in self.data_ip_list])
        self.get_instance_ip_list()

    def get_increase_status(self, sequence):
        if len(sequence.values) < INCREASE_MIN_LENGTH:
            return False
        try:
            anomalies = _continuous_increasing_detector.fit_predict(sequence)
            if True in anomalies.values:
                return True
        except:
            return False
        return False

    def get_forecast_status(self, sequence, forecast_time=DEFAULT_FORECAST_TIME,
                            upper_threshold=DEFAULT_UPPER_THRESHOLD, lower_threshold=DEFAULT_LOWER_THRESHOLD):
        forecast_sequence = quickly_forecast(sequence, forecast_time, given_parameters=DEFAULT_FORECAST_PARAM)
        current_timestamp = int(time.time() * 1000)
        if not is_sequence_valid(forecast_sequence):
            return False, {'occur_time': '', 'remaining_hours': 0.0, 'risk': '', 'timestamps': [], 'values': []}
        for timestamp, value in zip(forecast_sequence.timestamps, forecast_sequence.values):
            if value >= MAX_FORECAST_NUM:
                break
            if lower_threshold <= value <= upper_threshold:
                continue
            flag = 'future upper' if value > upper_threshold else 'future lower'
            remaining_hours = round((timestamp - current_timestamp) / 1000 / 60 / 60, 4)
            occur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp / 1000)))
            return True, {'occur_time': occur_time, 'remaining_hours': remaining_hours, 'risk': flag, 'timestamps':
                forecast_sequence.timestamps, 'values': forecast_sequence.values}
        return False, {'occur_time': '', 'remaining_hours': 0.0, 'risk': '', 'timestamps': [], 'values': []}

    def get_threshold_status(self, sequence,
                             upper_threshold=DEFAULT_UPPER_THRESHOLD,
                             lower_threshold=DEFAULT_LOWER_THRESHOLD,
                             percentage=None):
        res = []
        over_upper_threshold = False
        threshold_detector = anomaly_detection.ThresholdDetector(
            low=lower_threshold,
            high=upper_threshold,
            percentage=percentage
        )
        anomalies = threshold_detector.fit_predict(sequence)
        for timestamp, value, flag in zip(sequence.timestamps, sequence.values, anomalies.values):
            if not flag:
                continue

            if value > upper_threshold:
                res.append({'risk': 'upper', 'timestamp': timestamp, 'value': value})
                over_upper_threshold = True
            elif value < lower_threshold:
                res.append({'risk': 'lower', 'timestamp': timestamp, 'value': value})

        if res:
            return True, res, over_upper_threshold

        return False, res, over_upper_threshold

    def get_ftype_status(self, sequence):
        fstype = sequence.labels.get('fstype', 'UNKNOWN')
        if fstype == 'UNKNOWN':
            return False
        if fstype not in ('xfs', 'ext3', 'ext4'):
            return True
        return False

    def get_target_status(self, sequence, target):
        timestamp_list = []
        value_list = []
        for timestamp, value in zip(sequence.timestamps, sequence.values):
            if value == target:
                continue
            timestamp_list.append(timestamp)
            value_list.append(value)
        if len(timestamp_list) > 0:
            return True, timestamp_list, value_list
        return False, [], []

    def get_warning_dict(self, sequence, inspection_items):
        warning_dict = {}
        warning_status = False
        over_upper_threshold = False
        if (
            'threshold' in inspection_items.keys() and
            {'upper_threshold', 'lower_threshold'}.issubset(inspection_items['threshold'].keys())
        ):
            tmp_status, threshold_warning_list, over_upper_threshold = self.get_threshold_status(
                sequence,
                inspection_items['threshold']['upper_threshold'],
                inspection_items['threshold']['lower_threshold'],
                inspection_items['threshold'].get('percentage', None),
            )
            if tmp_status:
                warning_status = True
                warning_dict['threshold_warning'] = threshold_warning_list

        if over_upper_threshold and 'increase' in inspection_items.keys() and inspection_items['increase']:
            if self.get_increase_status(sequence):
                warning_status = True
                warning_dict['increase_warning'] = True

        if (
            'forecast' in inspection_items.keys() and
            {'forecast_time', 'upper_threshold', 'lower_threshold'}.issubset(inspection_items['forecast'].keys())
        ):
            tmp_status, forecast_warning_list = self.get_forecast_status(
                sequence,
                inspection_items['forecast']['forecast_time'],
                inspection_items['forecast']['upper_threshold'],
                inspection_items['forecast']['lower_threshold']
            )
            if tmp_status:
                warning_status = True
                warning_dict['forecast_warning'] = forecast_warning_list

        if 'ftype' in inspection_items.keys() and inspection_items['ftype']:
            if self.get_ftype_status(sequence):
                warning_status = True
                warning_dict['ftype_warning'] = True

        if 'target' in inspection_items.keys():
            tmp_status, timestamp_list, value_list = self.get_target_status(sequence, inspection_items['target'])
            if tmp_status:
                warning_status = True
                warning_dict['timestamps'] = timestamp_list
                warning_dict['value'] = value_list

        return warning_status, warning_dict

    def downsample_data(self, sequence):
        if len(sequence.values) < DEFAULT_DOWNSAMPLE_LENGTH:
            return sequence.timestamps, sequence.values
        sample_rate = int(len(sequence.values) / DEFAULT_DOWNSAMPLE_LENGTH)
        timestamp_list = sequence.timestamps[::sample_rate]
        value_list = sequence.values[::sample_rate]
        return timestamp_list, value_list

    def generate_inspection_result(self, sequence, inspection_items, abnormal_count, instance_dict, instance_key):
        sequence.values = [0.0 if value < 0 else value for value in sequence.values]
        avg_val, min_val, max_val, the_95th_val = get_statistic_data(sequence.values)
        warning_status, warning_dict = self.get_warning_dict(sequence, inspection_items)
        if warning_status:
            abnormal_count += 1
        timestamp_list, value_list = self.downsample_data(sequence)
        instance_dict[instance_key] = {'statistic': {'max': max_val, 'min': min_val, 'avg': avg_val, 'the_95th':
            the_95th_val}, 'warnings': warning_dict, 'timestamps': timestamp_list, 'data': value_list}
        return abnormal_count

    def get_instance_ip_list(self):
        nic_state = dai.get_latest_metric_sequence('opengauss_nic_state', WINDOW_IN_MINUTES).fetchall()
        data_to_management_dict, management_to_data_dict = get_ip_map_dict()
        global_data_ip_list = data_to_management_dict.keys()
        global_management_ip_list = management_to_data_dict.keys()
        for state in nic_state:
            nic_list = json.loads(state.labels.get('ip', "[]"))
            if not nic_list:
                continue
            nic_list = [split_ip_port(ip)[0] for ip in nic_list]
            for instance in self._instances_with_no_port:
                if instance in nic_list:
                    ip_dict[instance] = nic_list
                    if instance in global_data_ip_list:
                        ip_dict[data_to_management_dict[instance]] = nic_list
                    elif instance in global_management_ip_list:
                        ip_dict[management_to_data_dict[instance]] = nic_list
        if not nic_state:
            for data_ip, management_ip in data_to_management_dict.items():
                for instance in self._instances_with_no_port:
                    if instance == data_ip or instance == management_ip:
                        ip_dict[data_ip] = [data_ip, management_ip]
                        ip_dict[management_ip] = [data_ip, management_ip]

    @staticmethod
    def get_instance_regular_expression(ip, port=None):
        if not isinstance(port, (str, int)):
            port_pattern = ""
        elif port == PORT_SUFFIX:
            port_pattern = port
        elif port.isdigit() or isinstance(port, int):
            port_pattern = f":{port}"
        else:
            raise ValueError(f"Illegal port： {port}.")

        if ip not in ip_dict.keys():
            if port_pattern == PORT_SUFFIX:
                return f"{prepare_ip(ip)}{port_pattern}|{ip}"
            elif port_pattern:
                return f"{prepare_ip(ip)}{port_pattern}"
            else:
                return ip

        nic_list = []
        for nic_ip in ip_dict[ip]:
            if port_pattern == PORT_SUFFIX:
                nic_list.append(f"{prepare_ip(nic_ip)}{port_pattern}|{nic_ip}")
            elif port_pattern:
                nic_list.append(f"{prepare_ip(nic_ip)}{port_pattern}")
            else:
                nic_list.append(nic_ip)

        return '|'.join(nic_list)

    def get_instance_result_no_port(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            regular_instance = self.get_instance_regular_expression(instance)
            sequence = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).fetchone()
            if not is_sequence_valid(sequence):
                continue
            instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_instance_result_port(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_port:
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            sequence = dai.get_metric_sequence(
                metric_name, self._start, self._end, self._step
            ).from_server_like(regular_instance).fetchone()
            if not is_sequence_valid(sequence):
                continue
            instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_instance_result_filter_like(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            regular_instance = self.get_instance_regular_expression(instance, PORT_SUFFIX)
            sequence = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).filter_like(
                instance=regular_instance).fetchone()
            if not is_sequence_valid(sequence):
                continue
            instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_sys_disk_usage(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            regular_instance = self.get_instance_regular_expression(instance)
            sequences = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).fetchall()
            for sequence in sequences:
                if not is_sequence_valid(sequence):
                    continue
                mountpoint = sequence.labels.get('mountpoint', 'UNKNOWN')
                if mountpoint == 'UNKNOWN' or mountpoint != '/':
                    continue
                instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                          instance_abnormal_count, instance_dict,
                                                                          instance)
        return instance_abnormal_count, instance_dict

    def get_valid_sequence(self, cwd_sequence, data_directory):
        if not is_sequence_valid(cwd_sequence):
            return False, ''
        cwd_directory = cwd_sequence.labels.get('cwd', 'UNKNOWN')
        if cwd_directory == 'UNKNOWN':
            return False, ''
        if cwd_directory == data_directory:
            return True, ''
        if data_directory not in cwd_directory:
            return False, ''
        path_index = cwd_directory.find(data_directory)
        if path_index == -1:
            return False, ''
        prefix = cwd_directory[:path_index]
        if prefix + data_directory != cwd_directory:
            return False, ''
        return True, prefix

    def get_change_root_info(self, instance_ip, instance_port, data_directory):
        regular_instance = self.get_instance_regular_expression(instance_ip, PORT_SUFFIX)
        cwd_sequences = dai.get_metric_sequence(
            'opengauss_process_cpu_usage',
            self._start,
            self._end,
            self._step
        ).filter_like(instance=regular_instance).filter(port=instance_port).fetchall()

        for cwd_sequence in cwd_sequences:
            func_status, prefix = self.get_valid_sequence(cwd_sequence, data_directory)
            if func_status:
                return func_status, prefix

        return False, ''

    def get_instance_directory(self):
        dir_dict = {}
        for instance in self._instances_with_port:
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            directory_sequences = dai.get_metric_sequence(
                'pg_node_info_uptime',
                self._start,
                self._end,
                self._step
            ).from_server_like(regular_instance).fetchall()
            for directory_sequence in directory_sequences:
                if not is_sequence_valid(directory_sequence):
                    continue
                from_instance = directory_sequence.labels.get('from_instance', 'UNKNOWN')
                if from_instance == 'UNKNOWN':
                    continue
                instance_ip, instance_port = split_ip_port(from_instance)
                data_to_management_dict, management_to_data_dict = get_ip_map_dict()
                if instance_ip in data_to_management_dict.keys():
                    instance_ip = data_to_management_dict[instance_ip]
                from_instance = f"{prepare_ip(instance_ip)}:{instance_port}"
                data_directory = directory_sequence.labels.get('datapath', 'UNKNOWN')
                log_directory = directory_sequence.labels.get('log_directory', 'UNKNOWN')
                func_status, prefix = self.get_change_root_info(instance_ip, instance_port, data_directory)
                # record prefix
                if not func_status:
                    if from_instance in prefix_dict.keys():
                        prefix = prefix_dict[from_instance]
                    else:
                        continue
                else:
                    prefix_dict[from_instance] = prefix
                data_directory = prefix + data_directory
                if not log_directory.startswith('/'):
                    log_directory = data_directory + '/' + log_directory
                else:
                    log_directory = prefix + log_directory
                directory_info = (from_instance, data_directory, log_directory)
                if instance_ip in dir_dict.keys():
                    dir_dict[instance_ip].append(directory_info)
                else:
                    dir_dict[instance_ip] = [directory_info]
        return dir_dict

    def match_paths(self, directory, mount):
        if mount == "/":
            return 0.5
        n_matches = 0
        dir_list = directory.split('/')[1:]
        mount_dir_list = mount.split('/')[1:]
        min_len = min(len(dir_list), len(mount_dir_list))
        for idx, mount_directory in enumerate(mount_dir_list):
            if idx >= min_len:
                break
            if mount_directory == dir_list[idx]:
                n_matches += 1
        return n_matches

    def get_path_match_index(self, directory, sequences, instance_ip):
        max_matches = 0
        match_index = -1
        for index, sequence in enumerate(sequences):
            if not is_sequence_valid(sequence):
                continue
            from_instance = sequence.labels.get('from_instance', 'UNKNOWN')
            if from_instance == 'UNKNOWN':
                continue
            mountpoint = sequence.labels.get('mountpoint', 'UNKNOWN')
            matches = self.match_paths(directory, mountpoint)
            if matches > max_matches:
                max_matches = matches
                match_index = index
        return match_index

    def get_database_disk_usage(self, config_name, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        dir_dict = self.get_instance_directory()
        for instance_ip, directory_info_list in dir_dict.items():
            regular_instance = self.get_instance_regular_expression(instance_ip)
            sequences = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).fetchall()
            for directory_info in directory_info_list:
                if config_name == 'data_directory':
                    directory = directory_info[1]
                elif config_name == 'log_directory':
                    directory = directory_info[2]
                else:
                    continue
                match_index = self.get_path_match_index(directory, sequences, instance_ip)
                if match_index < 0 or match_index >= len(sequences):
                    continue
                instance_abnormal_count = self.generate_inspection_result(sequences[match_index], inspection_items,
                                                                          instance_abnormal_count, instance_dict,
                                                                          directory_info[0])
        return instance_abnormal_count, instance_dict

    def get_db_result(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        ip, port = split_ip_port(self._agent_instance)
        regular_instance = self.get_instance_regular_expression(ip, port)
        sequences = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
            regular_instance).fetchall()
        for sequence in sequences:
            if not is_sequence_valid(sequence):
                continue
            dbname = sequence.labels.get('datname', 'UNKNOWN')
            instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, dbname)
        return instance_abnormal_count, instance_dict

    def union_sequence(self, source_sequence, target_sequence):
        """union two sequences"""
        source_times = source_sequence.timestamps
        source_values = source_sequence.values
        target_times = target_sequence.timestamps
        target_values = target_sequence.values
        if any(item in target_times for item in source_times):
            return source_sequence
        union_times = source_times + target_times
        union_values = source_values + target_values
        zipped = zip(union_times, union_values)
        sort_zipped = sorted(zipped, key=lambda x: x[0])
        sort_result = zip(*sort_zipped)
        sorted_times, sorted_values = [list(x) for x in sort_result]
        new_sequence = Sequence(
            timestamps=sorted_times,
            values=sorted_values,
            name=source_sequence.name,
            step=source_sequence.step,
            labels=source_sequence.labels
        )
        return new_sequence

    def get_network_packet_loss(self, metric_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            regular_instance = self.get_instance_regular_expression(instance, PORT_SUFFIX)
            sequences = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).fetchall()
            sequence_dict = {}
            for sequence in sequences:
                if not is_sequence_valid(sequence):
                    continue
                source = sequence.labels.get('source', 'UNKNOWN')
                target = sequence.labels.get('target', 'UNKNOWN')
                if source == 'UNKNOWN' or target == 'UNKNOWN':
                    continue
                same_nic = False
                if source in self._instances_with_no_port and target in self._instances_with_no_port:
                    same_nic = True
                if source in self.data_ip_with_no_port and target in self.data_ip_with_no_port:
                    same_nic = True
                if not same_nic:
                    continue
                sequence.values = [(1 - item) for item in sequence.values]
                union_key = source + '_' + target
                if union_key in sequence_dict.keys():
                    source_sequence = sequence_dict[union_key]
                    sequence_dict[union_key] = self.union_sequence(source_sequence, sequence)
                else:
                    sequence_dict[union_key] = sequence
            for union_key, sequence in sequence_dict.items():
                instance_loss_dict = {}
                split_ips = union_key.split('_')
                if len(split_ips) != 2:
                    continue
                source, target = split_ips[0], split_ips[1]
                instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                          instance_abnormal_count, instance_loss_dict,
                                                                          target)
                if not instance_loss_dict:
                    continue
                if source in instance_dict.keys():
                    instance_dict[source].update(instance_loss_dict)
                else:
                    instance_dict[source] = instance_loss_dict
        return instance_abnormal_count, instance_dict

    def get_ip_state_from_sequence(self, sequence, label, normal_list):
        node_list = sequence.labels.get(label)
        state_dict = dict()
        abnormal_count = 0
        if not node_list:
            return abnormal_count, state_dict
        for item in json.loads(node_list):
            state_dict[item.get('ip')] = item.get('state')
            if item.get('state') not in normal_list:
                abnormal_count += 1
        return abnormal_count, state_dict

    def parse_opengauss_cluster_state(self, cluster_sequence):
        cluster_state = dict()
        cms_normal_list = ['Primary', 'Standby', 'Init']
        dn_normal_list = ['Normal']
        etcd_normal_list = ['StateFollower', 'StateLeader']
        cn_normal_list = ['Main Standby', 'Cascade Standby', 'Primary', 'Standby', 'Normal', 'Secondary']
        gtm_normal_list = ['Connection ok']
        abnormal_count = 0
        cms_abnormal_count, cluster_state['cms_state'] = self.get_ip_state_from_sequence(cluster_sequence, 'cms_state',
                                                                                         cms_normal_list)
        abnormal_count += cms_abnormal_count
        dn_abnormal_count, cluster_state['dn_state'] = self.get_ip_state_from_sequence(cluster_sequence, 'dn_state',
                                                                                       dn_normal_list)
        abnormal_count += dn_abnormal_count
        etcd_abnormal_count, cluster_state['etcd_state'] = self.get_ip_state_from_sequence(cluster_sequence,
                                                                                           'etcd_state',
                                                                                           etcd_normal_list)
        abnormal_count += etcd_abnormal_count
        if 'cn_state' in cluster_sequence.labels.keys():
            cent_abnormal_count, cluster_state['central_cn_state'] = self.get_ip_state_from_sequence(cluster_sequence,
                                                                                                     'central_cn_state',
                                                                                                     cn_normal_list)
            abnormal_count += cent_abnormal_count
            cn_abnormal_count, cluster_state['cn_state'] = self.get_ip_state_from_sequence(cluster_sequence, 'cn_state',
                                                                                           cn_normal_list)
            abnormal_count += cn_abnormal_count
            gtm_abnormal_count, cluster_state['gtm_state'] = self.get_ip_state_from_sequence(cluster_sequence,
                                                                                             'gtm_state',
                                                                                             gtm_normal_list)
            abnormal_count += gtm_abnormal_count
        return abnormal_count, cluster_state

    def get_node_status_from_metadatabase(self, inspection_items, role='dn'):
        cluster_state_dict = dict()
        abnormal_count = 0
        for instance in self._instances_with_no_port:
            if instance in ip_dict.keys():
                timestamp_list, value_list = get_cluster_diagnosis_records(ip_dict[instance],
                                                                           self._start, self._end,
                                                                           role)
            else:
                timestamp_list, value_list = get_cluster_diagnosis_records(instance, self._start,
                                                                           self._end, role)
            raw_sequence = RecordSequence(timestamp_list, value_list)
            result_timestamp_list, result_value_list = self.downsample_data(raw_sequence)
            cluster_state_instance_result = dict()
            cluster_state_instance_result['role'] = role
            cluster_state_instance_result['status'] = dict()
            cluster_state_instance_result['status']['timestamps'] = result_timestamp_list
            cluster_state_instance_result['status']['value'] = result_value_list

            downsample_sequence = RecordSequence(result_timestamp_list, result_value_list)
            warning_status, warning_dict = self.get_warning_dict(downsample_sequence, inspection_items)
            cluster_state_instance_result['warnings'] = warning_dict if warning_status else {}
            cluster_state_dict[instance] = cluster_state_instance_result
            if result_value_list and result_value_list[-1] != -1:
                abnormal_count += 1
        return abnormal_count, cluster_state_dict

    def get_top_querys(self):
        with global_vars.agent_proxy.context(self._agent_instance, self._username, self._password):
            res = global_vars.agent_proxy.call('query_in_postgres', TOP_QUERIES_SQL)
            return 0, res

    def long_transaction(self, duration=LONG_TRANSACTION_DURATION):
        with global_vars.agent_proxy.context(self._agent_instance, self._username, self._password):
            stmt = LONG_TRANSACTION_SQL.format(duration)
            res = global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)
            return len(res), res

    def get_core_dump_detail(self):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            regular_instance = self.get_instance_regular_expression(instance, PORT_SUFFIX)
            sequences = dai.get_metric_sequence('opengauss_log_ffic', self._start, self._end, MIN_STEP).filter_like(
                instance=regular_instance).fetchall()
            instance_timestamps = []
            instance_data = []
            core_dump_count = 0
            for sequence in sequences:
                if not is_sequence_valid(sequence):
                    continue
                instance_abnormal_count += 1
                core_dump_count += 1
                instance_timestamps.extend(sequence.timestamps)
                instance_data.extend(sequence.values)
            if core_dump_count > 0:
                instance_dict[instance] = {'count': core_dump_count, 'timestamps': instance_timestamps,
                                           'data': instance_data}
        return instance_abnormal_count, instance_dict

    def get_label_info_use(self, metric_name, label_name, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_port:
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            sequence = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).filter(type=label_name).fetchone()
            if not is_sequence_valid(sequence):
                continue
            instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_label_info_rate(self, metric_name, label_name_used, label_name_max, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_port:
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            used_sequence = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).filter(type=label_name_used).fetchone()
            if not is_sequence_valid(used_sequence):
                continue
            max_sequence = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).filter(type=label_name_max).fetchone()
            if not is_sequence_valid(max_sequence):
                continue
            max_memory = max_sequence.values[-1]
            if max_memory == 0:
                used_sequence.values = [item * 0.0 for item in used_sequence.values]
            else:
                used_sequence.values = [round(item / max_memory, 4) for item in used_sequence.values]
            instance_abnormal_count = self.generate_inspection_result(used_sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_active_session_rate(self, inspection_items):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_port:
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            total_sequence = dai.get_metric_sequence('opengauss_total_connection', self._start, self._end,
                                                     self._step).from_server_like(regular_instance).fetchone()
            if not is_sequence_valid(total_sequence):
                continue
            active_sequence = dai.get_metric_sequence('opengauss_active_connection', self._start, self._end,
                                                      self._step).from_server_like(regular_instance).fetchone()
            if not is_sequence_valid(active_sequence):
                continue
            if len(total_sequence.values) != len(active_sequence.values):
                continue
            rate_values = []
            for index, value in enumerate(active_sequence.values):
                if total_sequence.values[index] == 0:
                    rate_values.append(0.0)
                else:
                    rate_values.append(round(value / total_sequence.values[index], 4))
            active_sequence.values = rate_values
            instance_abnormal_count = self.generate_inspection_result(active_sequence, inspection_items,
                                                                      instance_abnormal_count, instance_dict, instance)
        return instance_abnormal_count, instance_dict

    def get_log_error(self):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_no_port:
            error_count = 0
            regular_instance = self.get_instance_regular_expression(instance, PORT_SUFFIX)
            error_type_dict = {}
            for error_type in FULL_LOG_ERROR_LIST:
                metric_name = 'opengauss_log_' + error_type
                sequences = dai.get_metric_sequence(metric_name, self._start, self._end, MIN_STEP).filter_like(
                    instance=regular_instance).fetchall()
                if len(sequences) < 1:
                    continue

                if error_type == 'errors_rate':
                    for sequence in sequences:
                        if max(sequence.values) > 0:
                            error_count += 1
                else:
                    error_count += len(sequences)

                error_count += len(sequences)
                if error_type in error_type_dict.keys():
                    error_type_dict[error_type] += len(sequences)
                else:
                    error_type_dict[error_type] = len(sequences)
            if error_count > 0:
                instance_abnormal_count += 1
                instance_dict[instance] = {'error_count': error_count, 'error_types': error_type_dict}

        return instance_abnormal_count, instance_dict

    def get_warning_info(self, increase_status=False, threshold_list=None, forecast_list=None, fstype_status=False,
                         target_status=None):
        warning_info_dict = {}
        if increase_status:
            warning_info_dict['increase'] = True
        if threshold_list:
            warning_info_dict['threshold'] = {'lower_threshold': threshold_list[0],
                                              'upper_threshold': threshold_list[1],
                                              'percentage': None}

        if forecast_list:
            warning_info_dict['forecast'] = {'forecast_time': forecast_list[0],
                                             'lower_threshold': forecast_list[1],
                                             'upper_threshold': forecast_list[2]}
        if fstype_status:
            warning_info_dict['ftype'] = True
        if target_status is not None:
            warning_info_dict['target'] = target_status
        return warning_info_dict

    def get_recommend_params(self, best_param, cur_param, vartype='integer'):
        recommend_dict = {}
        warning_status = False
        if vartype in ('integer', 'int64', 'real'):
            recommend_dict['opt_param'] = best_param
            recommend_dict['cur_param'] = cur_param
            recommend_dict['recommend_scope'] = [best_param * RECOMMEND_LOWER_RATE, best_param * RECOMMEND_UPPER_RATE]
            if cur_param < best_param * RECOMMEND_LOWER_RATE or cur_param > best_param * RECOMMEND_UPPER_RATE:
                recommend_dict['warning'] = True
                warning_status = True
            else:
                recommend_dict['warning'] = False
        elif vartype == 'bool':
            recommend_dict['opt_param'] = True if best_param else False
            recommend_dict['cur_param'] = True if cur_param else False
            recommend_dict['recommend_scope'] = []
            if cur_param + best_param == 0:
                recommend_dict['warning'] = False
            elif (cur_param + best_param) not in [cur_param, best_param]:
                recommend_dict['warning'] = False
            else:
                recommend_dict['warning'] = True
                warning_status = True
        return warning_status, recommend_dict

    def get_guc_params(self):
        instance_dict = {}
        instance_abnormal_count = 0
        for instance in self._instances_with_port:
            guc_dict = {}
            error_count = 0
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, PORT_SUFFIX)
            server_instance = self.get_instance_regular_expression(ip, port)
            total_memory_sequence = dai.get_latest_metric_value('os_mem_total_bytes').filter_like(
                instance=regular_instance).fetchone()
            if not is_sequence_valid(total_memory_sequence):
                continue
            # transfer bytes to unit 'GB'
            total_memory = total_memory_sequence.values[-1] / 1024 / 1024 / 1024
            # config for centralized
            if total_memory > 256:
                best_process_memory = total_memory * 0.875
            elif total_memory < 128:
                best_process_memory = total_memory * 0.625
            else:
                best_process_memory = total_memory * 0.75
            best_shared_buffers = total_memory / 4
            # work mem unit 'MB'
            if total_memory >= 128:
                best_work_mem = 128
            elif total_memory < 16:
                best_work_mem = 16
            else:
                best_work_mem = total_memory
            process_sequence = dai.get_latest_metric_value('pg_settings_setting').from_server_like(
                server_instance).filter(name='max_process_memory').fetchone()
            if not is_sequence_valid(process_sequence):
                continue
            cur_process_memory = process_sequence.values[-1] / 1024 / 1024
            process_vartype = process_sequence.labels.get('vartype', 'UNKNOWN')
            warning_status, guc_dict['max_process_memory'] = self.get_recommend_params(best_process_memory,
                                                                                       cur_process_memory,
                                                                                       process_vartype)
            if warning_status:
                error_count += 1
            buffer_sequence = dai.get_latest_metric_value('pg_settings_setting').from_server_like(
                server_instance).filter(name='shared_buffers').fetchone()
            if not is_sequence_valid(buffer_sequence):
                continue
            cur_shared_buffer = buffer_sequence.values[-1] * 8 / 1024 / 1024
            buffer_vartype = buffer_sequence.labels.get('vartype', 'UNKNOWN')
            warning_status, guc_dict['shared_buffers'] = self.get_recommend_params(best_shared_buffers,
                                                                                   cur_shared_buffer, buffer_vartype)
            if warning_status:
                error_count += 1
            work_mem_sequence = dai.get_latest_metric_value('pg_settings_setting').from_server_like(
                server_instance).filter(name='work_mem').fetchone()
            if not is_sequence_valid(work_mem_sequence):
                continue
            cur_work_mem = work_mem_sequence.values[-1] / 1024
            work_mem_vartype = work_mem_sequence.labels.get('vartype', 'UNKNOWN')
            warning_status, guc_dict['work_mem'] = self.get_recommend_params(best_work_mem, cur_work_mem,
                                                                             work_mem_vartype)
            if warning_status:
                error_count += 1
            instance_dict[instance] = guc_dict
            if error_count > 0:
                instance_abnormal_count += 1
        return instance_abnormal_count, instance_dict

    def get_statistic_info(self, metric_name, count_list, score_dict):
        if len(count_list) >= 1:
            max_count = max(count_list)
            score_dict['full_score'] += SCORE_WEIGHT_DICT[metric_name]
            if not max_count:
                score_dict['health_score'] += SCORE_WEIGHT_DICT[metric_name]
            else:
                score_dict['count'][metric_name] = max_count

    def get_inspection_result_single(self, inspection_name, metric_name, score_dict, warning_info_dict,
                                     extra_metric_list=None):
        if extra_metric_list is None:
            extra_metric_list = []
        inspection_dict = {}
        abnormal_count = 0
        warning_info = self.get_warning_info(*warning_info_dict[inspection_name])
        if inspection_name in ['os_disk_usage']:
            abnormal_count, inspection_dict[inspection_name] = self.get_sys_disk_usage(metric_name, warning_info)
        elif inspection_name in ['os_mem_usage', 'os_disk_ioutils']:
            abnormal_count, inspection_dict[inspection_name] = self.get_instance_result_no_port(metric_name,
                                                                                                warning_info)
        elif inspection_name == 'network_packet_loss':
            abnormal_count, inspection_dict[inspection_name] = self.get_network_packet_loss(metric_name, warning_info)
        elif inspection_name == 'component_error':
            abnormal_count, inspection_dict[inspection_name] = self.get_node_status_from_metadatabase(warning_info)
        elif inspection_name in ['data_directory', 'log_directory']:
            abnormal_count, inspection_dict[inspection_name] = self.get_database_disk_usage(inspection_name,
                                                                                            metric_name, warning_info)
        elif inspection_name in ['db_size', 'buffer_hit_rate', 'db_tmp_file', 'db_deadlock']:
            abnormal_count, inspection_dict[inspection_name] = self.get_db_result(metric_name, warning_info)
        elif inspection_name == 'active_session_rate':
            abnormal_count, inspection_dict[inspection_name] = self.get_active_session_rate(warning_info)
        elif inspection_name == 'log_error_check':
            abnormal_count, inspection_dict[inspection_name] = self.get_log_error()
        elif inspection_name in ['thread_pool', 'xmin_stuck']:
            abnormal_count, inspection_dict[inspection_name] = self.get_instance_result_port(metric_name, warning_info)
        elif inspection_name in ['xlog_accumulate']:
            abnormal_count, inspection_dict[inspection_name] = self.get_instance_result_filter_like(metric_name,
                                                                                                    warning_info)
        elif inspection_name == 'db_top_query':
            abnormal_count, inspection_dict[inspection_name] = self.get_top_querys()
        elif inspection_name == 'long_transaction':
            abnormal_count, inspection_dict[inspection_name] = self.long_transaction()
        elif inspection_name == 'core_dump':
            abnormal_count, inspection_dict[inspection_name] = self.get_core_dump_detail()
        elif inspection_name in ['process_memory']:
            abnormal_count, inspection_dict[inspection_name] = self.get_label_info_rate(extra_metric_list[0],
                                                                                        metric_name,
                                                                                        extra_metric_list[1],
                                                                                        warning_info)
        elif inspection_name in ['other_memory']:
            abnormal_count, inspection_dict[inspection_name] = self.get_label_info_use(extra_metric_list[0],
                                                                                       metric_name, warning_info)
        elif inspection_name == 'guc_params':
            abnormal_count, inspection_dict[inspection_name] = self.get_guc_params()
        self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
        return inspection_dict

    def get_inspection_result_list(self, inspection_name, sub_insp_list, metric_name_list, score_dict,
                                   warning_info_dict, extra_metric_list=None):
        if extra_metric_list is None:
            extra_metric_list = []
        inspection_dict = {inspection_name: {}}
        count_list = []
        for index, sub_inspection in enumerate(sub_insp_list):
            sub_warning_info = self.get_warning_info(*warning_info_dict[sub_inspection])
            if sub_inspection in ['cpu_user', 'cpu_iowait']:
                abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.get_instance_result_no_port(
                    metric_name_list[index], sub_warning_info)
            elif sub_inspection in ['login', 'logout', 'p95', 'p80', 'select', 'update', 'insert', 'delete', 'tps',
                                    'qps']:
                abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.get_instance_result_port(
                    metric_name_list[index], sub_warning_info)
            elif sub_inspection in ['commit', 'rollback']:
                abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.get_db_result(
                    metric_name_list[index], sub_warning_info)
            elif sub_inspection in ['dynamic_used_memory', 'dynamic_used_shrctx']:
                abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.get_label_info_rate(
                    extra_metric_list[0], metric_name_list[index], extra_metric_list[1], sub_warning_info)
            else:
                continue
            count_list.append(abnormal_count)
        self.get_statistic_info(inspection_name, count_list, score_dict)
        return inspection_dict

    def get_warning_params_increase(self, inspection_item, inspection_warning, warning_list, warning_params):
        if 'increase' in warning_list:
            if inspection_item not in INCREASE_INSP_ITEM_LIST:
                raise ValueError('The {} inspection does not support increase warning.'.format(inspection_item))
            if not isinstance(inspection_warning['increase'], bool):
                raise ValueError('The increase warning type of {} inspection must be bool.'.format(inspection_item))
            warning_params.append(inspection_warning['increase'])
        else:
            warning_params.append(False)

    def get_warning_params_threshold(self, inspection_item, inspection_warning, warning_list, warning_params):
        if 'threshold' in warning_list:
            if inspection_item not in THRESHOLD_INSP_ITEM_LIST:
                raise ValueError('The {} inspection does not support threshold warning.'.format(inspection_item))
            if not isinstance(inspection_warning['threshold'], list):
                raise ValueError('The threshold warning type of {} inspection must be list.'.format(inspection_item))
            for threshold_num in inspection_warning['threshold']:
                if not (isinstance(threshold_num, int) or isinstance(threshold_num, float)):
                    raise ValueError('The type of {} inspection threshold warning info must be int or float.'.format(
                        inspection_item))
            if len(inspection_warning['threshold']) == 2:
                if inspection_warning['threshold'][0] >= inspection_warning['threshold'][1]:
                    raise ValueError(
                        'For {} inspection threshold warning info, the first num must lower than last.'.format(
                            inspection_item))
                warning_params.append([inspection_warning['threshold'][0], inspection_warning['threshold'][1]])
            else:
                raise ValueError(
                    'The threshold warning info of {} inspection must be [lower_threshold, upper_threshold].'.format(
                        inspection_item))
        else:
            warning_params.append([])

    def get_warning_params_forecast(self, inspection_item, inspection_warning, warning_list, warning_params):
        if 'forecast' in warning_list:
            if inspection_item not in FORECAST_INSP_ITEM_LIST:
                raise ValueError('The {} inspection does not support forecast warning.'.format(inspection_item))
            if not isinstance(inspection_warning['forecast'], list):
                raise ValueError('The forecast warning type of {} inspection must be list.'.format(inspection_item))
            for forecast_num in inspection_warning['forecast']:
                if not (isinstance(forecast_num, int) or isinstance(forecast_num, float)):
                    raise ValueError(
                        'The type of {} inspection forecast warning info must be int or float.'.format(inspection_item))
            if len(inspection_warning['forecast']) == 3:
                if inspection_warning['forecast'][0] <= 0 or inspection_warning['forecast'][0] > 48 * 60:
                    raise ValueError(
                        'The {} inspection forecast time ranges from 0 to 2880, the unit is minute.'.format(
                            inspection_item))
                if inspection_warning['forecast'][1] >= inspection_warning['forecast'][2]:
                    raise ValueError(
                        'For {} inspection forecast warning info, the mid num must lower than last.'.format(
                            inspection_item))
                warning_params.append([inspection_warning['forecast'][0], inspection_warning['forecast'][1],
                                       inspection_warning['forecast'][2]])
            else:
                raise ValueError(
                    'The forecast warning info of {} inspection '
                    'must be [forecast_time, lower_threshold, upper_threshold].'.format(
                        inspection_item))
        else:
            warning_params.append([])

    def get_warning_params_ftype(self, inspection_item, inspection_warning, warning_list, warning_params):
        if 'ftype' in warning_list:
            if inspection_item not in FTYPE_INSP_ITEM_LIST:
                raise ValueError('The {} inspection does not support ftype warning.'.format(inspection_item))
            if not isinstance(inspection_warning['ftype'], bool):
                raise ValueError('The ftype warning type of {} inspection must be bool.'.format(inspection_item))
            warning_params.append(inspection_warning['ftype'])
        else:
            warning_params.append(False)

    def get_warning_params(self, inspection_item, inspection_warning):
        warning_params = []
        warning_list = inspection_warning.keys()
        self.get_warning_params_increase(inspection_item, inspection_warning, warning_list, warning_params)
        self.get_warning_params_threshold(inspection_item, inspection_warning, warning_list, warning_params)
        self.get_warning_params_forecast(inspection_item, inspection_warning, warning_list, warning_params)
        self.get_warning_params_ftype(inspection_item, inspection_warning, warning_list, warning_params)
        return warning_params

    def parse_warning_info_couple(self, inspection_item, inspection_warning, warning_info_dict):
        intersections = [x for x in COUPLE_INSP_ITEM_DICT[inspection_item] if x in inspection_warning.keys()]
        if len(intersections) == 0:
            for sub_inspection_item in COUPLE_INSP_ITEM_DICT[inspection_item]:
                warning_info_dict[sub_inspection_item] = self.get_warning_params(inspection_item, inspection_warning)
        elif intersections == COUPLE_INSP_ITEM_DICT[inspection_item] and len(inspection_warning.keys()) == len(
                intersections):
            for sub_inspection_item in COUPLE_INSP_ITEM_DICT[inspection_item]:
                if isinstance(inspection_warning[sub_inspection_item], bool):
                    warning_info_dict[sub_inspection_item] = WARNING_INFO_DICT[sub_inspection_item] if \
                        inspection_warning[sub_inspection_item] else []
                    continue
                if not isinstance(inspection_warning[sub_inspection_item], dict):
                    raise ValueError(
                        'The sub customized warning info of {} inspection must be dict.'.format(inspection_item))
                warning_info_dict[sub_inspection_item] = self.get_warning_params(inspection_item, inspection_warning[
                    sub_inspection_item])
        else:
            raise ValueError('The warning info of {} inspection is wrong.'.format(inspection_item))

    def get_customize_warning_info(self, inspection_items):
        inspection_list = []
        warning_info_dict = defaultdict(list)
        if len(inspection_items) == 0:
            return inspection_list, warning_info_dict
        if all(isinstance(item, str) for item in inspection_items):
            warning_info_dict = WARNING_INFO_DICT
            inspection_list = inspection_items
            return inspection_list, warning_info_dict
        inspection_items_dict = inspection_items[0]
        for inspection_item, inspection_warning in inspection_items_dict.items():
            if inspection_item in FIXED_INSP_ITEM_LIST:
                warning_info_dict[inspection_item] = WARNING_INFO_DICT[inspection_item]
            elif isinstance(inspection_warning, bool):
                if inspection_item in COUPLE_INSP_ITEM_DICT.keys():
                    for sub_inspection_item in COUPLE_INSP_ITEM_DICT[inspection_item]:
                        warning_info_dict[sub_inspection_item] = WARNING_INFO_DICT[
                            sub_inspection_item] if inspection_warning else []
                elif inspection_item in FULL_INSP_ITEM_LIST:
                    warning_info_dict[inspection_item] = WARNING_INFO_DICT[
                        inspection_item] if inspection_warning else []
            elif not isinstance(inspection_warning, dict):
                raise ValueError('The customized warning info of {} inspection must be dict.'.format(inspection_item))
            elif inspection_item in COUPLE_INSP_ITEM_DICT.keys():
                self.parse_warning_info_couple(inspection_item, inspection_warning, warning_info_dict)
            else:
                warning_info_dict[inspection_item] = self.get_warning_params(inspection_item, inspection_warning)
            inspection_list.append(inspection_item)
        return inspection_list, warning_info_dict

    def system_resource(self, sys_res_insp_items, score_dict):
        system_resource_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(sys_res_insp_items)
        if 'os_cpu_usage' in inspection_list:
            inspection_dict = self.get_inspection_result_list('os_cpu_usage', ['cpu_user', 'cpu_iowait'],
                                                              ['os_cpu_user_usage', 'os_cpu_iowait_usage'], score_dict,
                                                              warning_info_dict)
            system_resource_dict.update(inspection_dict)
        if 'os_disk_usage' in inspection_list:
            inspection_dict = self.get_inspection_result_single('os_disk_usage', 'os_disk_usage', score_dict,
                                                                warning_info_dict)
            system_resource_dict.update(inspection_dict)
        if 'os_mem_usage' in inspection_list:
            inspection_dict = self.get_inspection_result_single('os_mem_usage', 'os_mem_usage', score_dict,
                                                                warning_info_dict)
            system_resource_dict.update(inspection_dict)
        if 'os_disk_ioutils' in inspection_list:
            inspection_dict = self.get_inspection_result_single('os_disk_ioutils', 'os_disk_ioutils', score_dict,
                                                                warning_info_dict)
            system_resource_dict.update(inspection_dict)
        if 'network_packet_loss' in inspection_list:
            inspection_dict = self.get_inspection_result_single('network_packet_loss', 'opengauss_ping_packet_rate',
                                                                score_dict, warning_info_dict)
            system_resource_dict.update(inspection_dict)
        return system_resource_dict

    def instance_status(self, instance_status_indicators, score_dict):
        instance_status_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(instance_status_indicators)
        if 'component_error' in inspection_list:
            inspection_dict = self.get_inspection_result_single('component_error', 'component_error', score_dict,
                                                                warning_info_dict)
            instance_status_dict.update(inspection_dict)
        return instance_status_dict

    def database_resource(self, data_res_insp_items, score_dict):
        database_resource_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(data_res_insp_items)
        if 'data_directory' in inspection_list:
            inspection_dict = self.get_inspection_result_single('data_directory', 'os_disk_usage', score_dict,
                                                                warning_info_dict)
            database_resource_dict.update(inspection_dict)
        if 'log_directory' in inspection_list:
            inspection_dict = self.get_inspection_result_single('log_directory', 'os_disk_usage', score_dict,
                                                                warning_info_dict)
            database_resource_dict.update(inspection_dict)
        if 'db_size' in inspection_list:
            inspection_dict = self.get_inspection_result_single('db_size', 'pg_database_size_bytes', score_dict,
                                                                warning_info_dict)
            database_resource_dict.update(inspection_dict)
        return database_resource_dict

    def database_performance(self, data_perf_insp_items, score_dict):
        database_performance_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(data_perf_insp_items)
        if 'buffer_hit_rate' in inspection_list:
            inspection_dict = self.get_inspection_result_single('buffer_hit_rate', 'pg_db_blks_access', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'user_login_out' in inspection_list:
            inspection_dict = self.get_inspection_result_list('user_login_out', ['login', 'logout'],
                                                              ['opengauss_user_login', 'opengauss_user_logout'], score_dict,
                                                              warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'active_session_rate' in inspection_list:
            inspection_dict = self.get_inspection_result_single('active_session_rate', 'active_session_rate',
                                                                score_dict, warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'log_error_check' in inspection_list:
            inspection_dict = self.get_inspection_result_single('log_error_check', 'log_error_check', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'thread_pool' in inspection_list:
            inspection_dict = self.get_inspection_result_single('thread_pool', 'pg_thread_pool_rate', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_latency' in inspection_list:
            inspection_dict = self.get_inspection_result_list('db_latency', ['p95', 'p80'],
                                                              ['statement_responsetime_percentile_p95',
                                                               'statement_responsetime_percentile_p80'], score_dict,
                                                              warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_transaction' in inspection_list:
            inspection_dict = self.get_inspection_result_list('db_transaction', ['commit', 'rollback'],
                                                              ['pg_db_xact_commit', 'pg_db_xact_rollback'], score_dict,
                                                              warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_tmp_file' in inspection_list:
            inspection_dict = self.get_inspection_result_single('db_tmp_file', 'pg_db_temp_files', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_exec_statement' in inspection_list:
            inspection_dict = self.get_inspection_result_list('db_exec_statement',
                                                              ['select', 'update', 'insert', 'delete'],
                                                              ['pg_sql_count_select', 'pg_sql_count_update',
                                                               'pg_sql_count_insert', 'pg_sql_count_delete'],
                                                              score_dict, warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_deadlock' in inspection_list:
            inspection_dict = self.get_inspection_result_single('db_deadlock', 'pg_db_deadlocks', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_tps' in inspection_list:
            inspection_dict = self.get_inspection_result_list('db_tps', ['tps', 'qps'],
                                                              ['opengauss_qps_by_instance', 'qps'], score_dict,
                                                              warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'db_top_query' in inspection_list:
            inspection_dict = self.get_inspection_result_single('db_top_query', 'db_top_query', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'long_transaction' in inspection_list:
            inspection_dict = self.get_inspection_result_single('long_transaction', 'long_transaction', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'xmin_stuck' in inspection_list:
            inspection_dict = self.get_inspection_result_single('xmin_stuck', 'oldestxmin_increase', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        if 'xlog_accumulate' in inspection_list:
            inspection_dict = self.get_inspection_result_single('xlog_accumulate', 'opengauss_xlog_count', score_dict,
                                                                warning_info_dict)
            database_performance_dict.update(inspection_dict)
        return database_performance_dict

    def diagnosis_optimization(self, diag_opt_insp_items, score_dict):
        diagnosis_optimization_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(diag_opt_insp_items)
        if 'core_dump' in inspection_list:
            inspection_dict = self.get_inspection_result_single('core_dump', 'core_dump', score_dict, warning_info_dict)
            diagnosis_optimization_dict.update(inspection_dict)
        if 'dynamic_memory' in inspection_list:
            inspection_dict = self.get_inspection_result_list('dynamic_memory',
                                                              ['dynamic_used_memory', 'dynamic_used_shrctx'],
                                                              ['dynamic_used_memory', 'dynamic_used_shrctx'],
                                                              score_dict, warning_info_dict,
                                                              ['pg_total_memory_detail_mbytes', 'max_dynamic_memory'])
            diagnosis_optimization_dict.update(inspection_dict)
        if 'process_memory' in inspection_list:
            inspection_dict = self.get_inspection_result_single('process_memory', 'process_used_memory', score_dict,
                                                                warning_info_dict,
                                                                ['pg_total_memory_detail_mbytes', 'max_process_memory'])
            diagnosis_optimization_dict.update(inspection_dict)
        if 'other_memory' in inspection_list:
            inspection_dict = self.get_inspection_result_single('other_memory', 'other_used_memory', score_dict,
                                                                warning_info_dict, ['pg_total_memory_detail_mbytes'])
            diagnosis_optimization_dict.update(inspection_dict)
        if 'guc_params' in inspection_list:
            inspection_dict = self.get_inspection_result_single('guc_params', 'guc_params', score_dict,
                                                                warning_info_dict)
            diagnosis_optimization_dict.update(inspection_dict)
        return diagnosis_optimization_dict

    def get_inspection_conclusion(self, score_dict):
        conclusion_dict = {}
        full_score = score_dict['full_score']
        health_score = score_dict['health_score']
        conclusion_dict['health_score'] = health_score
        conclusion_dict['full_score'] = full_score
        if full_score == 0:
            health_scale = 1.0
        else:
            health_scale = health_score / full_score
        if health_scale == 1.0:
            health_status = 'perfect'
        elif health_scale > 0.9:
            health_status = 'excellent'
        elif health_scale > 0.75:
            health_status = 'good'
        elif health_scale > 0.6:
            health_status = 'mediocre'
        else:
            health_status = 'bad'
        conclusion_dict['health_status'] = health_status
        topk_list = sorted(score_dict['count'].items(), key=lambda x: x[1] * SCORE_WEIGHT_DICT[x[0]], reverse=True)
        conclusion_dict['top3'] = topk_list[:3]
        return conclusion_dict

    def intelligent_inspection(self, inspection_items):
        result = {}
        score_dict = {
            'full_score': 0,
            'health_score': 0,
            'count': {}
        }
        result['system_resource'] = self.system_resource(inspection_items.system_resource, score_dict)
        result['instance_status'] = self.instance_status(inspection_items.instance_status, score_dict)
        result['database_resource'] = self.database_resource(inspection_items.database_resource, score_dict)
        result['database_performance'] = self.database_performance(inspection_items.database_performance, score_dict)
        result['diagnosis_optimization'] = self.diagnosis_optimization(inspection_items.diagnosis_optimization,
                                                                       score_dict)
        result['conclusion'] = self.get_inspection_conclusion(score_dict)
        return result

    def check_insp_item_complete(self, inspection_items):
        query_insp_item_list = []
        for inspection_class, inspection_results in inspection_items.items():
            if inspection_class not in (
                'system_resource',
                'database_resource',
                'instance_status',
                'database_performance',
                'diagnosis_optimization'
            ):
                continue

            for inspection_item, _ in inspection_results.items():
                if inspection_item == 'guc_params' or inspection_item == 'active_session_rate':
                    continue
                query_insp_item_list.append(inspection_item)

        if set(query_insp_item_list) == set(FULL_INSP_ITEM_LIST):
            return True

        return False

    def check_daily_report_exist(self, instance, start_time, end_time, report_num):
        inspection_results = sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=dao.regular_inspections.select_metric_regular_inspections,
            instances=self.data_ip_list,
            start=int(start_time.timestamp() * 1000),
            end=int(end_time.timestamp() * 1000),
            inspection_type='daily_check'
        )
        index_of_start = inspection_results['header'].index('start')
        index_of_report = inspection_results['header'].index('report')
        inspection_results['rows'].sort(key=lambda x: x[index_of_start])
        valid_inspection_results = {'header': inspection_results['header']}
        valid_inspection_rows = []
        tmp_time = 0
        for row in inspection_results['rows']:
            if row[index_of_start] == tmp_time:
                continue
            if not self.check_insp_item_complete(row[index_of_report]):
                continue
            valid_inspection_rows.append(row)
            tmp_time = row[index_of_start]
        valid_inspection_results['rows'] = valid_inspection_rows
        if len(valid_inspection_rows) >= report_num:
            return True, valid_inspection_results
        return False, valid_inspection_results

    def union_instance_res(self, report, result):
        for record, record_info in report.items():
            if not record_info:
                continue
            if record not in result.keys():
                result[record] = RecordSequence([], [])
            result[record].extend_data(record_info['timestamps'], record_info['data'])
            if 'ftype_warning' in record_info['warnings'].keys():
                result[record].ftype = record_info['warnings']['ftype_warning']
        return

    def union_special_res(self, report, result, inspection_name):
        if inspection_name == 'component_error':
            self.union_component_error_result(report, result)
            return
        if inspection_name == 'long_transaction':
            result.extend(report)
            return
        for record, record_info in report.items():
            if not record_info:
                continue
            if record not in result.keys():
                if inspection_name == 'network_packet_loss':
                    result[record] = {}
                else:
                    result[record] = record_info
                    continue
            if inspection_name == 'log_error_check':
                result[record]['error_count'] += record_info['error_count']
                if 'error_types' not in record_info.keys():
                    continue
                if 'error_types' not in result[record].keys():
                    error_type_dict = {}
                    result[record]['error_types'] = error_type_dict
                else:
                    error_type_dict = result[record]['error_types']
                for error_type, error_type_count in record_info['error_types'].items():
                    if error_type in error_type_dict.keys():
                        error_type_dict[error_type] += error_type_count
                    else:
                        error_type_dict[error_type] = error_type_count
            elif inspection_name == 'core_dump':
                result[record]['count'] += record_info['count']
                result[record]['timestamps'].extend(record_info['timestamps'])
                result[record]['data'].extend(record_info['data'])
            elif inspection_name == 'network_packet_loss':
                for target, loss_info in record_info.items():
                    if not loss_info:
                        continue
                    if target not in result[record].keys():
                        result[record][target] = RecordSequence([], [])
                    result[record][target].extend_data(loss_info['timestamps'], loss_info['data'])
        return

    def union_component_error_result(self, report, result):
        for node_name, node_status in report.items():
            if not node_status:
                continue
            current_timestamps = node_status.get('status', {}).get('timestamps', [])
            current_value = node_status.get('status', {}).get('value', [])
            warn_timestamps = node_status.get('warnings', {}).get('timestamps', [])
            warn_value = node_status.get('warnings', {}).get('value', [])
            if node_name not in result.keys():
                result[node_name] = node_status
            else:
                if warn_timestamps:
                    if not result[node_name].get('warnings'):
                        result[node_name]['warnings']['timestamps'] = warn_timestamps
                        result[node_name]['warnings']['value'] = warn_value
                    else:
                        result[node_name]['warnings']['timestamps'].extend(warn_timestamps)
                        result[node_name]['warnings']['value'].extend(warn_value)
                result[node_name]['status']['timestamps'].extend(current_timestamps)
                result[node_name]['status']['value'].extend(current_value)

    def union_warning_info(self, record_dict, warning_info):
        result_dict = {}
        instance_abnormal_count = 0
        for record, record_info in record_dict.items():
            timestamp_value = zip(record_info.timestamps, record_info.values)
            sorted_timestamp_value = sorted(timestamp_value, key=lambda x: x[0])
            result = zip(*sorted_timestamp_value)
            sorted_timestamps, sorted_values = [list(x) for x in result]
            sequence = Sequence(sorted_timestamps, sorted_values)
            warning_status, warning_dict = self.get_warning_dict(sequence, warning_info)
            avg_val, min_val, max_val, the_95th_val = get_statistic_data(sequence.values)
            if 'ftype_warning' in warning_dict.keys() and record_info.ftype:
                warning_dict['ftype_warning'] = record_info.ftype
                warning_status = True
            if warning_status:
                instance_abnormal_count += 1
            timestamp_list, value_list = self.downsample_data(sequence)
            result_dict[record] = {
                'statistic': {'max': max_val, 'min': min_val, 'avg': avg_val, 'the_95th': the_95th_val},
                'warnings': warning_dict, 'timestamps': timestamp_list, 'data': value_list}
        return instance_abnormal_count, result_dict

    def union_inspection_result_single(self, inspection_name, data_dict, score_dict, warning_info_dict):
        inspection_dict = {}
        abnormal_count = 0
        warning_info = self.get_warning_info(*warning_info_dict[inspection_name])
        abnormal_count, inspection_dict[inspection_name] = self.union_warning_info(data_dict, warning_info)
        self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
        return inspection_dict

    def union_inspection_result_list(self, inspection_name, sub_insp_list, data_dict_list, score_dict,
                                     warning_info_dict):
        inspection_dict = {}
        inspection_dict[inspection_name] = {}
        count_list = []
        for index, sub_inspection in enumerate(sub_insp_list):
            sub_warning_info = self.get_warning_info(*warning_info_dict[sub_inspection])
            abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.union_warning_info(
                data_dict_list[index], sub_warning_info)
            count_list.append(abnormal_count)
        self.get_statistic_info(inspection_name, count_list, score_dict)
        return inspection_dict

    def union_inspection_result_special(self, inspection_name, data_dict, score_dict, warning_info_dict):
        inspection_dict = {}
        abnormal_count = 0
        warning_info = self.get_warning_info(*warning_info_dict[inspection_name])
        if inspection_name == 'log_error_check':
            for record, record_info in data_dict.items():
                if record_info['error_count'] > 0:
                    abnormal_count += 1
        elif inspection_name == 'core_dump':
            for record, record_info in data_dict.items():
                if record_info['count'] > 0:
                    abnormal_count += 1
        elif inspection_name == 'long_transaction':
            if len(data_dict) > 0:
                abnormal_count += 1
        elif inspection_name == 'component_error':
            for node_name, node_status in data_dict.items():
                if not node_status:
                    continue
                current_status_list = node_status.get('status').get('value')
                current_timestamps_list = node_status.get('status').get('timestamps')
                if current_status_list and current_status_list[-1] != -1:
                    abnormal_count += 1
                sequence = Sequence(current_timestamps_list, current_status_list)
                timestamp_list, value_list = self.downsample_data(sequence)
                node_status['status']['timestamps'] = timestamp_list
                node_status['status']['value'] = value_list
        self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
        inspection_dict[inspection_name] = data_dict
        return inspection_dict

    def union_network_package_loss_result(self, inspection_name, data_dict, score_dict, warning_info_dict):
        inspection_dict = {}
        inspection_dict[inspection_name] = {}
        count_list = []
        warning_info = self.get_warning_info(*warning_info_dict[inspection_name])
        for record, record_info in data_dict.items():
            abnormal_count, inspection_dict[inspection_name][record] = self.union_warning_info(record_info,
                                                                                               warning_info)
            count_list.append(abnormal_count)
        self.get_statistic_info(inspection_name, count_list, score_dict)
        return inspection_dict

    def union_system_resource(self, valid_inspection_reports, sys_res_insp_items, score_dict):
        cpu_user_dict = {}
        cpu_iowait_dict = {}
        os_disk_usage_dict = {}
        os_mem_usage_dict = {}
        os_disk_ioutils_dict = {}
        network_packet_loss_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(sys_res_insp_items)
        for valid_inspection_report in valid_inspection_reports:
            system_resource_report = valid_inspection_report['system_resource']
            os_cpu_usage_report = system_resource_report['os_cpu_usage']
            cpu_user_report = os_cpu_usage_report['cpu_user']
            cpu_iowait_report = os_cpu_usage_report['cpu_iowait']
            self.union_instance_res(cpu_user_report, cpu_user_dict)
            self.union_instance_res(cpu_iowait_report, cpu_iowait_dict)
            os_disk_usage_report = system_resource_report['os_disk_usage']
            self.union_instance_res(os_disk_usage_report, os_disk_usage_dict)
            os_mem_usage_report = system_resource_report['os_mem_usage']
            self.union_instance_res(os_mem_usage_report, os_mem_usage_dict)
            os_disk_ioutils_report = system_resource_report['os_disk_ioutils']
            self.union_instance_res(os_disk_ioutils_report, os_disk_ioutils_dict)
            network_packet_loss_report = system_resource_report['network_packet_loss']
            self.union_special_res(network_packet_loss_report, network_packet_loss_dict, 'network_packet_loss')
        system_resource_dict = {}
        inspection_dict = self.union_inspection_result_list('os_cpu_usage', ['cpu_user', 'cpu_iowait'],
                                                            [cpu_user_dict, cpu_iowait_dict], score_dict,
                                                            warning_info_dict)
        system_resource_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('os_disk_usage', os_disk_usage_dict, score_dict,
                                                              warning_info_dict)
        system_resource_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('os_mem_usage', os_mem_usage_dict, score_dict,
                                                              warning_info_dict)
        system_resource_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('os_disk_ioutils', os_disk_ioutils_dict, score_dict,
                                                              warning_info_dict)
        system_resource_dict.update(inspection_dict)
        inspection_dict = self.union_network_package_loss_result('network_packet_loss', network_packet_loss_dict,
                                                                 score_dict, warning_info_dict)
        system_resource_dict.update(inspection_dict)
        return system_resource_dict

    def union_instance_status(self, valid_inspection_reports, instance_status_indicators, score_dict):
        instance_status_dict = {}
        component_error_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(instance_status_indicators)
        for valid_inspection_report in valid_inspection_reports:
            instance_status_report = valid_inspection_report['instance_status']
            component_error_report = instance_status_report['component_error']
            self.union_special_res(component_error_report, component_error_dict, 'component_error')
        inspection_dict = self.union_inspection_result_special('component_error', component_error_dict, score_dict,
                                                               warning_info_dict)
        instance_status_dict.update(inspection_dict)
        return instance_status_dict

    def union_database_resource(self, valid_inspection_reports, data_res_insp_items, score_dict):
        data_directory_dict = {}
        log_directory_dict = {}
        db_size_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(data_res_insp_items)
        for valid_inspection_report in valid_inspection_reports:
            database_resource_report = valid_inspection_report['database_resource']
            data_directory_report = database_resource_report['data_directory']
            log_directory_report = database_resource_report['log_directory']
            db_size_report = database_resource_report['db_size']
            self.union_instance_res(data_directory_report, data_directory_dict)
            self.union_instance_res(log_directory_report, log_directory_dict)
            self.union_instance_res(db_size_report, db_size_dict)
        database_resource_dict = {}
        inspection_dict = self.union_inspection_result_single('data_directory', data_directory_dict, score_dict,
                                                              warning_info_dict)
        database_resource_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('log_directory', log_directory_dict, score_dict,
                                                              warning_info_dict)
        database_resource_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('db_size', db_size_dict, score_dict, warning_info_dict)
        database_resource_dict.update(inspection_dict)
        return database_resource_dict

    def union_database_performance(self, valid_inspection_reports, data_perf_insp_items, score_dict):
        buffer_hit_rate_dict = {}
        user_login_dict = {}
        user_logout_dict = {}
        active_session_rate_dict = {}
        log_error_check_dict = {}
        thread_pool_dict = {}
        p95_dict = {}
        p80_dict = {}
        commit_dict = {}
        rollback_dict = {}
        db_tmp_file_dict = {}
        select_dict = {}
        update_dict = {}
        insert_dict = {}
        delete_dict = {}
        db_deadlock_dict = {}
        tps_dict = {}
        qps_dict = {}
        long_transaction_list = []
        xmin_stuck_dict = {}
        xlog_accumulate_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(data_perf_insp_items)
        for valid_inspection_report in valid_inspection_reports:
            database_performance_report = valid_inspection_report['database_performance']
            buffer_hit_rate_report = database_performance_report['buffer_hit_rate']
            user_login_out_report = database_performance_report['user_login_out']
            user_login_report = user_login_out_report['login']
            user_logout_report = user_login_out_report['logout']
            active_session_rate_report = database_performance_report.get('active_session_rate', {})
            log_error_check_report = database_performance_report['log_error_check']
            thread_pool_report = database_performance_report['thread_pool']
            db_latency_report = database_performance_report['db_latency']
            p95_report = db_latency_report['p95']
            p80_report = db_latency_report['p80']
            db_transaction_report = database_performance_report['db_transaction']
            commit_report = db_transaction_report['commit']
            rollback_report = db_transaction_report['rollback']
            db_tmp_file_report = database_performance_report['db_tmp_file']
            db_exec_statement_report = database_performance_report['db_exec_statement']
            select_report = db_exec_statement_report['select']
            update_report = db_exec_statement_report['update']
            insert_report = db_exec_statement_report['insert']
            delete_report = db_exec_statement_report['delete']
            db_deadlock_report = database_performance_report['db_deadlock']
            db_tps_report = database_performance_report['db_tps']
            tps_report = db_tps_report['tps']
            qps_report = db_tps_report['qps']
            long_transaction_report = database_performance_report['long_transaction']
            xmin_stuck_report = database_performance_report['xmin_stuck']
            xlog_accumulate_report = database_performance_report['xlog_accumulate']
            self.union_instance_res(buffer_hit_rate_report, buffer_hit_rate_dict)
            self.union_instance_res(user_login_report, user_login_dict)
            self.union_instance_res(user_logout_report, user_logout_dict)
            self.union_instance_res(active_session_rate_report, active_session_rate_dict)
            self.union_special_res(log_error_check_report, log_error_check_dict, 'log_error_check')
            self.union_instance_res(thread_pool_report, thread_pool_dict)
            self.union_instance_res(p95_report, p95_dict)
            self.union_instance_res(p80_report, p80_dict)
            self.union_instance_res(commit_report, commit_dict)
            self.union_instance_res(rollback_report, rollback_dict)
            self.union_instance_res(db_tmp_file_report, db_tmp_file_dict)
            self.union_instance_res(select_report, select_dict)
            self.union_instance_res(update_report, update_dict)
            self.union_instance_res(insert_report, insert_dict)
            self.union_instance_res(delete_report, delete_dict)
            self.union_instance_res(db_deadlock_report, db_deadlock_dict)
            self.union_instance_res(tps_report, tps_dict)
            self.union_instance_res(qps_report, qps_dict)
            self.union_special_res(long_transaction_report, long_transaction_list, 'long_transaction')
            self.union_instance_res(xmin_stuck_report, xmin_stuck_dict)
            self.union_instance_res(xlog_accumulate_report, xlog_accumulate_dict)
        database_performance_dict = {}
        inspection_dict = self.union_inspection_result_single('buffer_hit_rate', buffer_hit_rate_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('user_login_out', ['login', 'logout'],
                                                            [user_login_dict, user_logout_dict], score_dict,
                                                            warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('active_session_rate', active_session_rate_dict,
                                                              score_dict, warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_special('log_error_check', log_error_check_dict, score_dict,
                                                               warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('thread_pool', thread_pool_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('db_latency', ['p95', 'p80'], [p95_dict, p80_dict],
                                                            score_dict, warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('db_transaction', ['commit', 'rollback'],
                                                            [commit_dict, commit_dict], score_dict, warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('db_tmp_file', db_tmp_file_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('db_exec_statement',
                                                            ['select', 'update', 'insert', 'delete'],
                                                            [select_dict, update_dict, insert_dict, delete_dict],
                                                            score_dict, warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('db_deadlock', db_deadlock_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('db_tps', ['tps', 'qps'], [tps_dict, qps_dict], score_dict,
                                                            warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.get_inspection_result_single('db_top_query', 'db_top_query', score_dict,
                                                            warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_special('long_transaction', long_transaction_list, score_dict,
                                                               warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('xmin_stuck', xmin_stuck_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('xlog_accumulate', xlog_accumulate_dict, score_dict,
                                                              warning_info_dict)
        database_performance_dict.update(inspection_dict)
        return database_performance_dict

    def union_diagnosis_optimization(self, valid_inspection_reports, diag_opt_insp_items, score_dict):
        core_dump_dict = {}
        dynamic_used_memory_dict = {}
        dynamic_used_shrctx_dict = {}
        process_memory_dict = {}
        other_memory_dict = {}
        inspection_list, warning_info_dict = self.get_customize_warning_info(diag_opt_insp_items)
        for valid_inspection_report in valid_inspection_reports:
            diagnosis_optimization_report = valid_inspection_report['diagnosis_optimization']
            core_dump_report = diagnosis_optimization_report['core_dump']
            dynamic_memory_report = diagnosis_optimization_report['dynamic_memory']
            dynamic_used_memory_report = dynamic_memory_report['dynamic_used_memory']
            dynamic_used_shrctx_report = dynamic_memory_report['dynamic_used_shrctx']
            process_memory_report = diagnosis_optimization_report['process_memory']
            other_memory_report = diagnosis_optimization_report['other_memory']
            self.union_special_res(core_dump_report, core_dump_dict, 'core_dump')
            self.union_instance_res(dynamic_used_memory_report, dynamic_used_memory_dict)
            self.union_instance_res(dynamic_used_shrctx_report, dynamic_used_shrctx_dict)
            self.union_instance_res(process_memory_report, process_memory_dict)
            self.union_instance_res(other_memory_report, other_memory_dict)
        diagnosis_optimization_dict = {}
        inspection_dict = self.union_inspection_result_special('core_dump', core_dump_dict, score_dict,
                                                               warning_info_dict)
        diagnosis_optimization_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_list('dynamic_memory',
                                                            ['dynamic_used_memory', 'dynamic_used_shrctx'],
                                                            [dynamic_used_memory_dict, dynamic_used_shrctx_dict],
                                                            score_dict, warning_info_dict)
        diagnosis_optimization_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('process_memory', process_memory_dict, score_dict,
                                                              warning_info_dict)
        diagnosis_optimization_dict.update(inspection_dict)
        inspection_dict = self.union_inspection_result_single('other_memory', other_memory_dict, score_dict,
                                                              warning_info_dict)
        diagnosis_optimization_dict.update(inspection_dict)
        inspection_dict = self.get_inspection_result_single('guc_params', 'guc_params', score_dict, warning_info_dict)
        diagnosis_optimization_dict.update(inspection_dict)
        return diagnosis_optimization_dict

    def get_report_result_union(self, valid_inspection_results, inspection_items):
        index_of_report = valid_inspection_results['header'].index('report')
        valid_inspection_reports = [row[index_of_report] for row in valid_inspection_results['rows']]
        result = {}
        if not valid_inspection_reports:
            return result
        score_dict = {
            'full_score': 0,
            'health_score': 0,
            'count': {}
        }
        result['system_resource'] = self.union_system_resource(valid_inspection_reports,
                                                               inspection_items.system_resource, score_dict)
        result['instance_status'] = self.union_instance_status(valid_inspection_reports,
                                                               inspection_items.instance_status, score_dict)
        result['database_resource'] = self.union_database_resource(valid_inspection_reports,
                                                                   inspection_items.database_resource, score_dict)
        result['database_performance'] = self.union_database_performance(valid_inspection_reports,
                                                                         inspection_items.database_performance,
                                                                         score_dict)
        result['diagnosis_optimization'] = self.union_diagnosis_optimization(valid_inspection_reports,
                                                                             inspection_items.diagnosis_optimization,
                                                                             score_dict)
        result['conclusion'] = self.get_inspection_conclusion(score_dict)
        return result


class CentralizeMultipleHoursInspection(MultipleHoursInspection):
    def __init__(self, instance, username, password, start=None, end=None, step=DEFAULT_STEP):
        super().__init__(instance, username, password, start, end, step)


class DistributeMultipleHoursInspection(MultipleHoursInspection):
    def __init__(self, instance, username, password, start=None, end=None, step=DEFAULT_STEP):
        super().__init__(instance, username, password, start, end, step)
        type_dict, instance_type_dict = get_instance_type_dict(instance, username, password)
        self.coordinator_list, self.datanode_list, self.standby_list = get_instance_type_management(
            self._instances_with_port, instance_type_dict)
        self.coordinator_no_port = [split_ip_port(i)[0] for i in self.coordinator_list]
        self.datanode_no_port = [split_ip_port(i)[0] for i in self.datanode_list]
        self.standby_no_port = [split_ip_port(i)[0] for i in self.standby_list]

    def get_db_result(self, metric_name, inspection_items):
        total_abnormal_count = 0
        total_dict = {}
        for instance in self._instances_with_port:
            instance_dict = {}
            instance_abnormal_count = 0
            ip, port = split_ip_port(instance)
            regular_instance = self.get_instance_regular_expression(ip, port)
            sequences = dai.get_metric_sequence(metric_name, self._start, self._end, self._step).from_server_like(
                regular_instance).fetchall()
            for sequence in sequences:
                if not is_sequence_valid(sequence):
                    continue
                dbname = sequence.labels.get('datname', 'UNKNOWN')
                instance_abnormal_count = self.generate_inspection_result(sequence, inspection_items,
                                                                          instance_abnormal_count, instance_dict,
                                                                          dbname)
            total_abnormal_count += instance_abnormal_count
            if instance_dict:
                total_dict[instance] = instance_dict
        return total_abnormal_count, total_dict

    def get_node_status_from_metadatabase(self, inspection_items):
        cluster_state_dict = dict()
        abnormal_count = 0
        dn_abnormal_count, dn_cluster_state_dict = super().get_node_status_from_metadatabase(inspection_items, 'dn')
        abnormal_count += dn_abnormal_count
        cluster_state_dict['dn'] = dn_cluster_state_dict
        cn_abnormal_count, cn_cluster_state_dict = super().get_node_status_from_metadatabase(inspection_items, 'cn')
        abnormal_count += cn_abnormal_count
        cluster_state_dict['cn'] = cn_cluster_state_dict
        return abnormal_count, cluster_state_dict

    def get_top_querys(self):
        instance_abnormal_count = 0
        instance_dict = {}
        for instance in self.coordinator_list:
            if instance not in global_vars.agent_proxy.agents:
                continue

            with global_vars.agent_proxy.context(instance, self._username, self._password):
                res = global_vars.agent_proxy.call('query_in_postgres', TOP_QUERIES_SQL)
                instance_dict[instance] = res
                instance_abnormal_count += 0
        return instance_abnormal_count, instance_dict

    def long_transaction(self, duration=LONG_TRANSACTION_DURATION):
        instance_abnormal_count = 0
        instance_dict = {}
        for instance in self.coordinator_list:
            if instance not in global_vars.agent_proxy.agents:
                continue

            with global_vars.agent_proxy.context(instance, self._username, self._password):
                stmt = LONG_TRANSACTION_SQL.format(duration)
                res = global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)
                instance_dict[instance] = res
                instance_abnormal_count += len(res)
        return instance_abnormal_count, instance_dict

    def union_db_res(self, record, record_info, result):
        if record not in result.keys():
            result[record] = {}
        for db_name, db_info in record_info.items():
            if not db_info:
                continue
            if 'timestamps' not in db_info.keys():
                continue
            if db_name not in result[record].keys():
                result[record][db_name] = RecordSequence([], [])
            result[record][db_name].extend_data(db_info['timestamps'], db_info['data'])
            if 'ftype_warning' in db_info['warnings'].keys():
                result[record][db_name].ftype = db_info['warnings']['ftype_warning']
        return

    def union_instance_res(self, report, result):
        for record, record_info in report.items():
            if not record_info:
                continue
            if 'timestamps' not in record_info.keys():
                self.union_db_res(record, record_info, result)
                continue
            if record not in result.keys():
                result[record] = RecordSequence([], [])
            result[record].extend_data(record_info['timestamps'], record_info['data'])
            if 'ftype_warning' in record_info['warnings'].keys():
                result[record].ftype = record_info['warnings']['ftype_warning']
        return

    def union_special_res(self, report, result, inspection_name):
        if inspection_name == 'component_error':
            for role, role_info in report.items():
                if role not in result.keys():
                    result[role] = {}
                self.union_component_error_result(role_info, result[role])
            return
        if inspection_name == 'long_transaction':
            if len(result) != 0:
                transaction_dict = result[0]
            else:
                transaction_dict = {}
                result.append(transaction_dict)
            for record, record_info in report.items():
                if not record_info:
                    continue
                if record not in transaction_dict.keys():
                    transaction_dict[record] = record_info
                else:
                    transaction_dict[record].extend(record_info)
            return
        super().union_special_res(report, result, inspection_name)
        return

    def union_inspection_result_single(self, inspection_name, data_dict, score_dict, warning_info_dict):
        inspection_dict = {}
        abnormal_count = 0
        warning_info = self.get_warning_info(*warning_info_dict[inspection_name])
        if inspection_name in ['db_size', 'buffer_hit_rate', 'db_tmp_file', 'db_deadlock']:
            inspection_dict[inspection_name] = {}
            for instance, instance_info in data_dict.items():
                instance_abnormal_count, instance_dict = self.union_warning_info(instance_info, warning_info)
                abnormal_count += instance_abnormal_count
                inspection_dict[inspection_name][instance] = instance_dict
        else:
            abnormal_count, inspection_dict[inspection_name] = self.union_warning_info(data_dict, warning_info)
        self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
        return inspection_dict

    def union_inspection_result_list(self, inspection_name, sub_insp_list, data_dict_list, score_dict,
                                     warning_info_dict):
        inspection_dict = {}
        inspection_dict[inspection_name] = {}
        count_list = []
        for index, sub_inspection in enumerate(sub_insp_list):
            sub_warning_info = self.get_warning_info(*warning_info_dict[sub_inspection])
            if inspection_name == 'db_transaction':
                abnormal_count = 0
                inspection_dict[inspection_name][sub_inspection] = {}
                for instance, instance_info in data_dict_list[index].items():
                    instance_abnormal_count, instance_dict = self.union_warning_info(instance_info, sub_warning_info)
                    abnormal_count += instance_abnormal_count
                    inspection_dict[inspection_name][sub_inspection][instance] = instance_dict
            else:
                abnormal_count, inspection_dict[inspection_name][sub_inspection] = self.union_warning_info(
                    data_dict_list[index], sub_warning_info)
            count_list.append(abnormal_count)
        self.get_statistic_info(inspection_name, count_list, score_dict)
        return inspection_dict

    def union_inspection_result_special(self, inspection_name, data_dict, score_dict, warning_info_dict):
        inspection_dict = {}
        abnormal_count = 0
        if inspection_name not in ['long_transaction', 'component_error']:
            inspection_dict = super().union_inspection_result_special(
                inspection_name, data_dict, score_dict, warning_info_dict
            )
            return inspection_dict

        if inspection_name == 'long_transaction':
            for instance, instance_info in data_dict[0].items():
                if len(instance_info) > 0:
                    abnormal_count += 1
            self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
            inspection_dict[inspection_name] = data_dict[0]
            return inspection_dict

        if inspection_name == 'component_error':
            for role, role_info in data_dict.items():
                for node_name, node_status in role_info.items():
                    if not node_status:
                        continue
                    current_status_list = node_status.get('status').get('value')
                    current_timestamps_list = node_status.get('status').get('timestamps')
                    if current_status_list and current_status_list[-1] != -1:
                        abnormal_count += 1
                    sequence = Sequence(current_timestamps_list, current_status_list)
                    timestamp_list, value_list = self.downsample_data(sequence)
                    node_status['status']['timestamps'] = timestamp_list
                    node_status['status']['value'] = value_list

        self.get_statistic_info(inspection_name, [abnormal_count], score_dict)
        inspection_dict[inspection_name] = data_dict
        return inspection_dict


def check_time_month_begin(check_time):
    return (check_time.day == 1 and check_time.hour == 0
            and check_time.minute == 0 and check_time.second == 0
            and check_time.microsecond == 0)


def check_time_day_begin(check_time):
    return (check_time.hour == 0
            and check_time.minute == 0
            and check_time.second == 0
            and check_time.microsecond == 0)


def check_time_days_valid(start_time, end_time, days=1):
    time_delta = end_time - start_time
    return (check_time_day_begin(start_time) and
            check_time_day_begin(end_time) and
            time_delta.days == days)


def check_time_month_valid(start_time, end_time):
    time_delta = end_time - start_time
    if check_time_day_begin(start_time) and check_time_day_begin(end_time) and time_delta.days == 30:
        return True, 30
    if check_time_month_begin(start_time) and check_time_month_begin(end_time):
        if (end_time.year == start_time.year) and (end_time.month - start_time.month == 1):
            return True, time_delta.days
        if (end_time.year - start_time.year == 1) and (end_time.month - start_time.month == -11):
            return True, time_delta.days
    return False, 0


def check_time_valid(inspection_type, start_time, end_time):
    time_delta = end_time - start_time
    if inspection_type == 'real_time_check':
        return True if (end_time > start_time) else False

    if inspection_type == 'daily_check':
        return check_time_days_valid(start_time, end_time)

    if inspection_type == 'weekly_check':
        return check_time_days_valid(start_time, end_time, 7)

    if inspection_type == 'monthly_check':
        return (check_time_day_begin(start_time) and
                check_time_day_begin(end_time) and
                time_delta.days >= 14)

    return False


def get_start_end_time(inspection_type, tz):
    if inspection_type == 'real_time_check':
        end_time = datetime.now(tz=tz)
        step_min = DEFAULT_STEP / 60 / 1000
        time_min = end_time.minute
        end_time = end_time.replace(minute=0, second=0, microsecond=0)
        minutes = int(((time_min + (step_min - 1)) // step_min) * step_min)
        end_time = end_time + timedelta(minutes=minutes)
        start_time = end_time - timedelta(seconds=SIX_HOUR_IN_SECONDS)
    elif inspection_type == 'daily_check':
        end_time = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(seconds=ONE_DAY_IN_SECONDS)
    elif inspection_type == 'weekly_check':
        end_time = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(seconds=ONE_WEEK_IN_SECONDS)
    elif inspection_type == 'monthly_check':
        end_time = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(seconds=ONE_MONTH_IN_SECONDS)
    else:
        end_time = datetime.now(tz=tz)
        start_time = end_time - timedelta(seconds=SIX_HOUR_IN_SECONDS)

    return start_time, end_time


def init_start_end_time(start_time, end_time, inspection_type, tz):
    if start_time is not None or end_time is not None:
        if not (isinstance(start_time, str) and start_time.isdigit() and len(start_time) == 13):
            raise ValueError('Incorrect value for parameter start_time: {}.'.format(start_time))
        if not (isinstance(end_time, str) and end_time.isdigit() and len(end_time) == 13):
            raise ValueError('Incorrect value for parameter end_time: {}.'.format(end_time))
        start_time = datetime.fromtimestamp(int(start_time) / 1000, tz=tz)
        end_time = datetime.fromtimestamp(int(end_time) / 1000, tz=tz)
        if not check_time_valid(inspection_type, start_time, end_time):
            raise ValueError(
                'The time interval between start_time and end_time is not suit for inspection_type: {}.'.format(
                    inspection_type))
    else:
        start_time, end_time = get_start_end_time(inspection_type, tz)
    return start_time, end_time


def real_time_inspection(username, password, inspection_type, start_time, end_time, instance, inspection_items, tz):
    results = []
    tz = adjust_timezone(tz)
    start_record = datetime.now(tz=tz)
    inspect_state = 'fail'
    error_msg = ''
    report_num = 0
    start_time, end_time = init_start_end_time(start_time, end_time, inspection_type, tz)
    if inspection_type == 'weekly_check':
        report_num = 7
    elif inspection_type == 'monthly_check':
        report_num = 14

    report = {}
    try:
        cluster_type = get_instance_cluster_type(instance, username, password)
        if cluster_type == 'centralize':
            inspector = CentralizeMultipleHoursInspection(instance, username, password, start_time, end_time)
        elif cluster_type == 'distribute':
            inspector = DistributeMultipleHoursInspection(instance, username, password, start_time, end_time)
        else:
            raise ValueError("can not get cluster type of instance: {}".format(instance))

        if inspection_type in ['daily_check', 'real_time_check']:
            report = inspector.intelligent_inspection(inspection_items)
            inspect_state = 'success'
        elif inspection_type in ['weekly_check', 'monthly_check']:
            exist_status, valid_inspection_results = inspector.check_daily_report_exist(instance, start_time, end_time,
                                                                                        report_num)
            report = inspector.get_report_result_union(valid_inspection_results, inspection_items)
            if exist_status:
                inspect_state = 'success'
            else:
                inspect_state = 'abnormal'
                error_msg = 'inspection failed because do not have enough daily_check.'

    except Exception as exception:
        report = {}
        logging.error('exec real_time inspection failed, because: %s', str(exception))
        error_msg = str(exception)

    end_record = datetime.now(tz=tz)
    cost_time = end_record - start_record
    results.append({'instance': instance,
                    'inspection_type': inspection_type,
                    'start': int(start_time.timestamp() * 1000),
                    'end': int(end_time.timestamp() * 1000),
                    'report': report,
                    'state': inspect_state,
                    'cost_time': cost_time.total_seconds(),
                    'conclusion': ''})

    try:
        dai.save_regular_inspection_results(results)
    except Exception as exception:
        logging.error('insert inspection result into database failed, because: %s', exception)

    if inspect_state != 'success':
        raise Exception(error_msg)

    return report

