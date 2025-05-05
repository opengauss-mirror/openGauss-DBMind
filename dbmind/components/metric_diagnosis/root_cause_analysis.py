# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
import copy
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from itertools import chain
from types import SimpleNamespace

from psycopg2.extensions import parse_dsn, make_dsn

from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.exceptions import DontIgnoreThisError
from dbmind.common.opengauss_driver import Driver
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils import dbmind_assert, string_to_dict
from dbmind.common.utils.checking import date_type, path_type, prepare_ip, split_ip_port, WITH_PORT
from dbmind.common.utils.component import initialize_tsdb_param
from dbmind.components.metric_diagnosis.rca_graph import RCA_GRAPH
from dbmind.components.metric_diagnosis.utils import get_metric
from dbmind.constants import PORT_SUFFIX
from dbmind.service import dai

LEAST_WINDOW = 300  # empirical
LEAST_N_INTERVALS = 40  # empirical
MAX_SCRAPE_LENGTH = 10000


def check_params(metric_name, metric_filter, alarm_cause=None, reason_name=None,
                 rca_params=None):
    def is_valid_str(s):
        return isinstance(s, str) and len(s) > 0

    all_metrics = list(
        chain.from_iterable(
            [analysis["main_metric_name"] for analysis in RCA_GRAPH.values()]
        )
    )
    if metric_name not in all_metrics:
        raise ValueError("Wrong metric name")

    dbmind_assert(is_valid_str(metric_filter), "metric_filter should be a string.")
    try:
        metric_filter_dict = json.loads(metric_filter)
    except json.decoder.JSONDecodeError:
        metric_filter_dict = string_to_dict(metric_filter)

    if not metric_filter_dict or not isinstance(metric_filter_dict, dict):
        raise DontIgnoreThisError("Illegal metric_filter: %s" % metric_filter)

    if not (is_valid_str(alarm_cause) or is_valid_str(reason_name)):
        raise DontIgnoreThisError("alarm_cause or reason_name should be a string.")

    if is_valid_str(alarm_cause) and is_valid_str(reason_name):
        raise DontIgnoreThisError("Please do not enter the two parameters 'alarm_cause' "
                                  "and 'reason_name' at the same time.")

    if is_valid_str(alarm_cause):
        try:
            alarm_cause_list = json.loads(alarm_cause)
        except json.decoder.JSONDecodeError:
            alarm_cause_list = [cause.strip() for cause in alarm_cause.split(",")]

        if not alarm_cause_list or not isinstance(alarm_cause_list, list):
            raise DontIgnoreThisError("Illegal alarm_cause: %s" % alarm_cause)

        for cause in alarm_cause_list:
            if cause not in RCA_GRAPH:
                raise DontIgnoreThisError("Wrong alarm cause: %s" % cause)

            elif metric_name not in RCA_GRAPH[cause].get("main_metric_name"):
                raise DontIgnoreThisError(
                    f"metrics mismatch: {metric_name} and "
                    f"{RCA_GRAPH[cause]['main_metric_name']}"
                )

        reason_name_list = []

    elif is_valid_str(reason_name):
        try:
            reason_name_list = json.loads(reason_name)
        except json.decoder.JSONDecodeError:
            reason_name_list = [reason.strip() for reason in reason_name.split(",")]

        if not reason_name_list or not isinstance(reason_name_list, list):
            raise DontIgnoreThisError("Illegal reason_name: %s" % reason_name)

        for reason_name in reason_name_list:
            for item in RCA_GRAPH.values():
                if reason_name in item["reasons"] and metric_name in item["main_metric_name"]:
                    break

            else:
                raise DontIgnoreThisError(f"Wrong reason_name {reason_name} for {metric_name}.")

        alarm_cause_list = []

    else:
        alarm_cause_list = []
        reason_name_list = []

    if is_valid_str(rca_params):
        try:
            rca_params_dict = json.loads(rca_params)
        except json.decoder.JSONDecodeError:
            raise DontIgnoreThisError(f"Wrong rca_params {rca_params} for {metric_name}.")
    else:
        rca_params_dict = {}

    return metric_filter_dict, alarm_cause_list, reason_name_list, rca_params_dict


def get_instance_attibutes(ip, start, end, interval):
    """ Configure the main metric ip address aliases list."""
    nic_states = dai.get_metric_sequence(
        "opengauss_nic_state",
        datetime.fromtimestamp(start),
        datetime.fromtimestamp(end),
        step=interval * 1000,
    ).fetchall()

    for nic_state in nic_states:
        ip_list = json.loads(nic_state.labels.get("ip"))
        if ip_list is None:
            continue

        if ip in ip_list:
            break
    else:
        logging.warning("The instance: %s was not in 'opengauss_nic_state'.", ip)
        ip_list = [ip]

    node_ip = "|".join(ip_list)
    node_ip_port = "|".join([f"{prepare_ip(ip)}{PORT_SUFFIX}|{ip}" for ip in ip_list])

    return {
        "regex_ip": node_ip,
        "regex_ip_port": node_ip_port,
        "main_ip_list": ip_list
    }


def get_mount_info(from_instance, cluster_mount_point):
    mount_info = {"cn": defaultdict(list), "dn": defaultdict(list)}
    for mount_state in cluster_mount_point:
        labels = mount_state.labels
        ip, port, role = labels.get("ip"), labels.get("port"), labels.get("role")
        device, file_system = labels.get("device"), labels.get("file_system")
        mount_info[role][f"{prepare_ip(ip)}:{port}"].append((device, file_system))

    devices_set, file_systems_set = set(), set()
    if from_instance in mount_info["dn"]:  # matching dn instance
        for device, file_system in mount_info["dn"][from_instance]:
            devices_set.add(device)
            file_systems_set.add(file_system)

    elif not WITH_PORT.match(from_instance):  # This metric is a system metric, its instance has no port.
        for role, instances in mount_info.items():
            for instance, devices in instances.items():
                if split_ip_port(instance)[0] != from_instance:
                    continue

                for device, file_system in devices:
                    devices_set.add(device)
                    file_systems_set.add(file_system)

    elif from_instance in mount_info["cn"]:
        for device, file_system in mount_info["cn"][from_instance]:
            devices_set.add(device)
            file_systems_set.add(file_system)

        for instance, devices in mount_info["dn"].items():
            if split_ip_port(instance)[0] != split_ip_port(from_instance)[0]:
                continue

            for device, file_system in devices:
                devices_set.add(device)
                file_systems_set.add(file_system)

    return {
        "devices": "|".join(devices_set),
        "file_systems": "|".join(file_systems_set)
    }


def adjust_start_and_end(start, end, interval):
    """check the params start and end and expand the time window
    :param start: start timestamp in millisecond
    :param end: end timestamp in millisecond
    :param interval: scrape interval in second
    """

    if start > end:
        raise ValueError("The start time must be earlier than end time.")

    window = interval * LEAST_N_INTERVALS  # second

    recent_end = int(end) // 1000  # second
    recent_end = min(int(datetime.now().timestamp()), recent_end + window)
    recent_start = min(end - window * 2, int(start) // 1000)  # second
    recent_start = max(recent_start, recent_end - MAX_SCRAPE_LENGTH * interval)

    beginning_start = int(start) // 1000 - window  # second
    beginning_end = max(beginning_start + window * 2, int(end) // 1000)  # second
    beginning_end = min(int(datetime.now().timestamp()), beginning_end)
    beginning_end = min(beginning_end, beginning_start + MAX_SCRAPE_LENGTH * interval)

    return recent_start, recent_end, beginning_start, beginning_end


def rca(metric_name: str, metric_filter: dict,
        start: int, end: int, tsdb_interval: int,
        alarm_cause_list: list = None,
        reason_name_list: list = None,
        rca_params_dict: dict = None):
    """Root Cause Analysis
    :param metric_name: metric_name
    :param metric_filter: dict of metric_filter
    :param start: start timestamp in millisecond
    :param end: end timestamp in millisecond
    :param tsdb_interval: scrape interval in second
    :param alarm_cause_list: list of alarm_causes, Optional
    :param reason_name_list: list of reason_names, Optional
    :param rca_params_dict: dict: given analysis params, Optional
    """
    if rca_params_dict is None:
        rca_params_dict = {}

    recent_start, recent_end, beginning_start, beginning_end = adjust_start_and_end(start, end, tsdb_interval)
    attributes = {
        "recent_start": recent_start,  # second
        "recent_end": recent_end,  # second
        "beginning_start": beginning_start,  # second
        "beginning_end": beginning_end,  # second
        "step": tsdb_interval  # second
    }

    features = dict()
    advices = dict()
    normal_seqs = defaultdict(list)
    abnormal_seqs = defaultdict(list)

    if alarm_cause_list:
        items = [RCA_GRAPH.get(cause) for cause in alarm_cause_list]
    elif reason_name_list:
        items = list()
        for reason_name in reason_name_list:
            for item in RCA_GRAPH.values():
                if reason_name in item["reasons"] and metric_name in item["main_metric_name"]:
                    break

            else:
                raise DontIgnoreThisError(f"Wrong reason_name {reason_name} for {metric_name}.")

            item = copy.deepcopy(item)
            item["reasons"] = {reason_name: item["reasons"][reason_name]}
            items.append(item)

    else:
        raise DontIgnoreThisError("Wrong alarm_cause or reason_name.")

    for item in items:
        main_metric_filter = copy.deepcopy(metric_filter)
        target_filter = item.get("main_metric_filter", {})
        min_length = item.get("min_length", 2)
        main_metric_filter.update(target_filter)
        # The recent sequence
        recent_main_seq = get_metric(
            metric_name,
            recent_start,
            recent_end,
            step=tsdb_interval * 1000,
            labels=main_metric_filter,
            fetch_all=False,
            min_length=min_length
        )
        # The beginning sequence
        beginning_main_seq = get_metric(
            metric_name,
            beginning_start,
            beginning_end,
            step=tsdb_interval * 1000,
            labels=main_metric_filter,
            fetch_all=False,
            min_length=min_length
        )
        source_flag = dai.get_metric_source_flag(metric_name)
        from_instance = recent_main_seq.labels.get(source_flag)  # exporter层ip
        main_ip = split_ip_port(from_instance)[0]
        attributes.update(get_instance_attibutes(main_ip, recent_start, recent_end, tsdb_interval))

        label_names_involved = {
            label_name
            for reason in item.get("reasons").values()
            for analyses in reason[0]
            for analysis in analyses
            for label_name in analysis.get("metric_filter_like", {})
        }
        if label_names_involved & {"device", "file_system"}:
            cluster_mount_point = dai.get_metric_sequence(
                "opengauss_mount_usage",
                datetime.fromtimestamp(recent_start),
                datetime.fromtimestamp(recent_end),
                step=tsdb_interval * 1000,
            ).filter_like(instance=attributes["regex_ip_port"]).fetchall()

            if not cluster_mount_point:
                raise ValueError("No sequence fetched for metric 'opengauss_mount_usage'.")

            attributes.update(get_mount_info(from_instance, cluster_mount_point))

        for name, (reason, advice) in item.get("reasons").items():
            features[name] = 0
            advices[name] = advice
            final_score = 0
            given_params = rca_params_dict.get(name, [])
            for analyses in reason:
                score = float('inf')
                for i, analysis in enumerate(analyses):
                    analyzer_args = SimpleNamespace(
                        metric_name=analysis.get("metric_name"),
                        metric_filter=analysis.get("metric_filter", {}),
                        metric_filter_like={
                            k: attributes.get(v)
                            for k, v in analysis.get("metric_filter_like", {}).items()
                            if attributes.get(v)
                        },
                        length=analysis.get("length_in_seconds"),
                        params=given_params[i] if given_params and given_params[i] else analysis.get("params"),
                        score=analysis.get("score"),
                        mode=analysis.get("mode"),
                        record=analysis.get("record", True),
                        recent_main_seq=recent_main_seq,
                        beginning_main_seq=beginning_main_seq,
                        recent_start=recent_start,
                        recent_end=recent_end,
                        beginning_start=beginning_start,
                        beginning_end=beginning_end,
                        step=tsdb_interval * 1000
                    )
                    method = analysis.get("method")(analyzer_args)
                    score = min(score, method.analyze())
                    normal_seqs[name].extend([seq.jsonify() for seq in method.related_seqs["normal"]])
                    abnormal_seqs[name].extend([seq.jsonify() for seq in method.related_seqs["abnormal"]])

                final_score = max(final_score, score)

            features[name] = round(max(features[name], final_score), 5)

    sum_score = sum(features.values())
    max_score = max(1e-5, max(features.values())) if features else 0
    conclusions = [reason for reason, score in features.items() if score == max_score]
    if sum_score:
        for reason in features:
            features[reason] = round(features[reason] / sum_score, 5)

    return (
        features,
        ",".join(conclusions) if conclusions else "Unknown",
        " Or ".join([advices[reason] for reason in conclusions]),
        dict(normal_seqs),
        dict(abnormal_seqs)
    )


def insight_view(metric_name: str, metric_filter: dict,
                 start: int, end: int, tsdb_interval: int,
                 alarm_cause_list: list = None,
                 reason_name_list: list = None,
                 driver: Driver = None, features: list = None):
    """ Insight
    :param metric_name: metric_name
    :param metric_filter: dict of metric_filter
    :param start: start timestamp in millisecond
    :param end: end timestamp in millisecond
    :param tsdb_interval: scrape interval in second
    :param alarm_cause_list: list of alarm_causes, Optional
    :param reason_name_list: list of reason_names, Optional
    :param driver: psycopg2 driver, Optional
    :param features: feature dict, Optional
    """
    recent_start, recent_end, beginning_start, beginning_end = adjust_start_and_end(start, end, tsdb_interval)
    attributes = {
        "driver": driver,  # the given driver
        "recent_start": recent_start,  # second
        "recent_end": recent_end,  # second
        "beginning_start": beginning_start,  # second
        "beginning_end": beginning_end,  # second
        "original_start": int(start) // 1000,  # second
        "step": tsdb_interval  # second
    }

    insight_output = dict()

    if alarm_cause_list:
        items = [RCA_GRAPH.get(cause) for cause in alarm_cause_list]
    elif reason_name_list:
        items = list()
        for reason_name in reason_name_list:
            for item in RCA_GRAPH.values():
                if reason_name in item["reasons"] and metric_name in item["main_metric_name"]:
                    break

            else:
                raise DontIgnoreThisError(f"Wrong reason_name {reason_name} for {metric_name}.")

            item = copy.deepcopy(item)
            if reason_name in item["insights"]:
                item["insights"] = {reason_name: item["insights"][reason_name]}
            else:
                item["insights"] = {}

            items.append(item)

    else:
        raise DontIgnoreThisError("Wrong alarm_cause or reason_name.")

    for item in items:
        main_metric_filter = copy.deepcopy(metric_filter)
        target_filter = item.get("main_metric_filter", {})
        main_metric_filter.update(target_filter)
        # To find the very sequence
        source_flag = dai.get_metric_source_flag(metric_name)
        from_instance = main_metric_filter.get(source_flag)  # exporter层ip
        main_ip = split_ip_port(from_instance)[0]
        attributes.update(get_instance_attibutes(main_ip, recent_start, recent_end, tsdb_interval))
        attributes["main_instance"] = from_instance
        attributes["metric_filter"] = metric_filter
        for name, insights in item.get("insights", {}).items():
            if isinstance(features, list) and name not in features:
                continue

            for insight in insights:
                insight_name = insight["name"]
                kwargs = {
                    k: attributes.get(v)
                    for k, v in insight.get("kwargs").items()
                    if attributes.get(v)
                }
                method = insight.get("method")(**kwargs)
                output = method.check()
                if not output:
                    logging.info("No output for %s with %s.", insight_name, kwargs)
                    continue

                insight_output[insight_name] = output

    return insight_output


def main(argv):
    parser = argparse.ArgumentParser(description="Cluster diagnosis.")
    parser.add_argument("--conf", required=True, type=path_type,
                        help="set the directory of configuration files")
    parser.add_argument("--metric", required=True,
                        help="set the metric name for diagnosis. ")
    parser.add_argument("--metric-filter", required=True,
                        help="set the filter for the metric. A list of label "
                             "(format is label=name) separated by comma(,).")
    parser.add_argument("--alarm-cause",
                        help="set the alarm cause for the metric. A list of "
                             "alarm cause separated by comma(,)."
                             "Choose from %s" % list(RCA_GRAPH.keys()))
    parser.add_argument("--start", required=True, type=date_type,
                        help="set the start time of the alarm in ms, supporting "
                             "UNIX-timestamp with microsecond or datetime format.")
    parser.add_argument("--end", required=True, type=date_type,
                        help="set the end time of the alarm in ms, supporting "
                             "UNIX-timestamp with microsecond or datetime format.")
    parser.add_argument("--reason-name",
                        help="set the reason name for the metric. A member of "
                             "alarm cause reasons.")
    parser.add_argument("--rca-params",
                        help="set the rca params for the analysis.")
    parser.add_argument("--url", metavar="DSN of database",
                        help="set database dsn('postgres://user@host:port/dbname' or "
                             "'user=user dbname=dbname host=host port=port') "
                             "when tsdb is not available. "
                             "Note: don't contain password in DSN for this diagnosis.")
    args = parser.parse_args(argv)

    os.chdir(args.conf)
    init_global_configs(args.conf)
    if not initialize_tsdb_param():
        parser.exit(1, "TSDB service does not exist, exiting...\n")

    client = TsdbClientFactory.get_tsdb_client()
    tsdb_interval = client.scrape_interval
    if not (isinstance(tsdb_interval, int) and tsdb_interval):
        tsdb_interval = LEAST_WINDOW // LEAST_N_INTERVALS

    metric_name = args.metric
    metric_filter = args.metric_filter
    alarm_cause = args.alarm_cause
    start = args.start
    end = args.end
    reason_name = args.reason_name
    rca_params = args.rca_params

    metric_filter, alarm_cause_list, reason_name_list, rca_params_dict = check_params(
        metric_name, metric_filter, alarm_cause, reason_name=reason_name, rca_params=rca_params
    )

    driver = None
    if args.url is not None:
        driver = Driver()
        try:
            parsed_dsn = parse_dsn(args.url)
            url = make_dsn(**parsed_dsn)
            driver.initialize(url)
        except ConnectionError:
            logging.error('Error occurred when initialized the URL, exiting...')

    res = rca(metric_name, metric_filter, start, end, tsdb_interval,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list,
              rca_params_dict=rca_params_dict)
    insights = insight_view(metric_name, metric_filter, start, end, tsdb_interval,
                            alarm_cause_list=alarm_cause_list,
                            reason_name_list=reason_name_list,
                            driver=driver)
    return res, insights
