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

import inspect
import json
import logging
from collections import defaultdict

from dbmind.app.monitoring.specific_detection import SpecificDetection
from dbmind.app.monitoring.generic_anomaly_detector import GenericAnomalyDetector
from dbmind.common.algorithm.anomaly_detection import detectors
from dbmind.common.types import ALARM_TYPES, ALARM_LEVEL
from dbmind.common.utils.checking import prepare_ip, split_ip_port, uniform_instance
from dbmind.constants import PORT_SUFFIX
from dbmind.metadatabase.dao.anomaly_detectors import (
    select_anomaly_detectors,
    delete_anomaly_detectors,
    update_anomaly_detectors
)
from dbmind.service.cluster_info import get_specific_agents
from dbmind.service.dai import save_anomaly_detectors

from .monitoring_constants import (
    DetectorAction,
    DetectorParam,
    AlarmInfo,
    DetectorInfo,
    get_monitoring_alarm_args,
    get_monitoring_detector_args
)

ALARM_INFO_ARGS = get_monitoring_alarm_args()
DETECTOR_INFO_ARGS = get_monitoring_detector_args()


def get_anomaly_detectors():
    meta_detectors = get_detectors_from_metadatabase()
    for cluster_name in meta_detectors:
        for meta_detector in meta_detectors[cluster_name].values():
            detector = meta_dict_to_cached_detactor(meta_detector)
            yield detector


def get_detectors_from_metadatabase(nodes=None, detector_name=None):
    cluster_result = select_anomaly_detectors(detector_name=detector_name)
    appeared_clusters = defaultdict(list)
    delete_ids = list()
    meta_detectors = dict()
    for tup in cluster_result:
        try:
            old_nodes = json.loads(tup.cluster_name)
            json.loads(tup.detector_info)
        except json.decoder.JSONDecodeError:
            delete_ids.append(tup.detector_id)
            continue

        if nodes is not None and not set(nodes) & set(old_nodes):
            continue

        duplicated = False
        for appeared_cluster_name, detector_names in appeared_clusters.items():
            appeared_nodes = json.loads(appeared_cluster_name)
            if set(old_nodes) & set(appeared_nodes) and tup.detector_name in detector_names:
                delete_ids.append(tup.detector_id)
                duplicated = True
                break

        if duplicated:
            continue

        cluster_name = json.dumps(sorted(nodes)) if nodes is not None else tup.cluster_name
        appeared_clusters[cluster_name].append(tup.detector_name)
        if cluster_name not in meta_detectors:
            meta_detectors[cluster_name] = dict()

        meta_detectors[cluster_name][tup.detector_name] = {
            DetectorParam.DETECTOR_ID: tup.detector_id,
            DetectorParam.CLUSTER_NAME: cluster_name,
            DetectorParam.DETECTOR_NAME: tup.detector_name,
            DetectorParam.ALARM_CAUSE: tup.alarm_cause,
            DetectorParam.ALARM_CONTENT: tup.alarm_content,
            DetectorParam.ALARM_LEVEL: tup.alarm_level,
            DetectorParam.ALARM_TYPE: tup.alarm_type,
            DetectorParam.EXTRA: tup.extra,
            DetectorParam.DETECTOR_INFO: tup.detector_info,
            DetectorParam.DURATION: tup.duration,
            DetectorParam.FORECASTING_SECONDS: tup.forecasting_seconds,
            DetectorParam.RUNNING: tup.running
        }

    delete_anomaly_detectors(list(delete_ids))

    return meta_detectors


def add_detectors_to_metadatabase(nodes: list, detector_name: str, meta_dict: dict):
    """Update the detector in meta database or insert the detector into meta database."""
    meta_detectors = get_detectors_from_metadatabase(nodes, detector_name)
    meta_detector = meta_dict.copy()
    if DetectorParam.DETECTOR_ID in meta_detector:
        meta_detector.pop(DetectorParam.DETECTOR_ID)

    if meta_detectors:
        for detector_dict in meta_detectors.values():
            for detector_name, detector in detector_dict.items():

                update_anomaly_detectors(detector[DetectorParam.DETECTOR_ID], **meta_detector)
    else:
        save_anomaly_detectors(meta_detector)


def delete_detectors_from_metadatabase(nodes, detector_name=None):
    """Delete detectors from meta database."""
    if detector_name is None:
        meta_detectors = get_detectors_from_metadatabase(nodes)
    else:
        meta_detectors = get_detectors_from_metadatabase(nodes, detector_name)

    delete_id_list = list()
    for detector_dict in meta_detectors.values():
        for detector_name, detector in detector_dict.items():
            delete_id_list.append(detector[DetectorParam.DETECTOR_ID])

    if not delete_id_list and detector_name is not None:
        raise KeyError

    delete_anomaly_detectors(delete_id_list)


def add_detector(primary, nodes, detector_name, json_dict, fuzzy_match=True):
    if fuzzy_match:
        instance_set = get_specific_agents(split_ip_port(primary)[0])
        instance = '|'.join([f"{prepare_ip(ip)}{PORT_SUFFIX}|{ip}" for ip in instance_set])
    else:
        instance = uniform_instance(primary)

    cluster_name = json.dumps(sorted(nodes))
    arguments = parsing_json_kwarg(json_dict)
    try:
        duration, forecasting_seconds, alarm_info, detector_info = arguments
        if not isinstance(detector_info[0].get("metric_filter"), dict):
            detector_info[0]["metric_filter"] = dict()

        detector_info[0]["metric_filter"]["from_instance"] = instance
        meta_dict = {
            DetectorParam.CLUSTER_NAME: cluster_name,
            DetectorParam.DETECTOR_NAME: detector_name,
            DetectorParam.ALARM_CAUSE: alarm_info.alarm_cause,
            DetectorParam.ALARM_CONTENT: alarm_info.alarm_content,
            DetectorParam.ALARM_LEVEL: alarm_info.alarm_level,
            DetectorParam.ALARM_TYPE: alarm_info.alarm_type,
            DetectorParam.EXTRA: alarm_info.extra,
            DetectorParam.DETECTOR_INFO: json.dumps(detector_info),
            DetectorParam.DURATION: duration,
            DetectorParam.FORECASTING_SECONDS: forecasting_seconds,
            DetectorParam.RUNNING: 1
        }
        add_detectors_to_metadatabase(nodes, detector_name, meta_dict)
        logging.info("AD_PoolManager: Successfully add detector %s for %s.", detector_name, nodes)
        return f"Success: add {detector_name} for {nodes}"

    except Exception as e:
        logging.exception(e)
        logging.info("AD_PoolManager: No new detectors added for %s.", nodes)
        return f"Failed: add {detector_name} for {nodes}"


def delete_detector(nodes, detector_name):
    cluster_name = json.dumps(sorted(nodes))
    try:
        delete_detectors_from_metadatabase(nodes, detector_name=detector_name)
        logging.info("AD_PoolManager: Successfully delete detector %s for %s.", detector_name, cluster_name)
        return f"Success: delete {detector_name} for {cluster_name}"
    except KeyError:
        logging.info("AD_PoolManager: Detector %s for %s is not found.", detector_name, cluster_name)
        return f"Failed: delete {detector_name} for {cluster_name}"
    except Exception as e:
        logging.exception(e)
        return f"Failed: delete {detector_name} for {cluster_name}"


def _adjust_detector(nodes, detector_name, action):
    running = 0 if action == "pause" else 1
    cluster_name = json.dumps(sorted(nodes))
    try:
        meta_detectors = get_detectors_from_metadatabase(nodes, detector_name)
        meta_dict = meta_detectors[cluster_name][detector_name]
        meta_dict[DetectorParam.RUNNING] = running
        add_detectors_to_metadatabase(nodes, detector_name, meta_dict)
        logging.info("AD_PoolManager: Successfully %s detector %s for %s.",
                     action, detector_name, cluster_name)
        return f"Success: {action} {detector_name} for {cluster_name}"
    except KeyError:
        logging.info("AD_PoolManager: Detector %s for %s is not found.",
                     detector_name, cluster_name)
        return f"Failed: {action} {detector_name} for {cluster_name}"
    except Exception as e:
        logging.exception(e)
        return f"Failed: {action} {detector_name} for {cluster_name}"


def pause_detector(nodes, detector_name):
    return _adjust_detector(nodes, detector_name, DetectorAction.PAUSE)


def resume_detector(nodes, detector_name):
    return _adjust_detector(nodes, detector_name, DetectorAction.RESUME)


def view_detector(nodes, detector_name):
    cluster_name = json.dumps(sorted(nodes))
    if detector_name == 'all':
        meta_detectors = get_detectors_from_metadatabase(nodes)
        meta_dicts = meta_detectors.get(cluster_name, {})
        return {name: meta_dict_to_proc_dict(meta_dict)
                for name, meta_dict in meta_dicts.items()}

    meta_detectors = get_detectors_from_metadatabase(nodes, detector_name)
    if not meta_detectors.get(cluster_name):
        return {}

    return meta_dict_to_proc_dict(meta_detectors[cluster_name].get(detector_name, {}))


def clear_detector(nodes):
    try:
        delete_detectors_from_metadatabase(nodes)
        return f"Success: clear detectors for {nodes}"
    except Exception as e:
        logging.exception(e)
        return f"Failed: clear detectors for {nodes}"


def clear_expired_detector(agents):
    meta_detectors = get_detectors_from_metadatabase()
    current_clusters = meta_detectors.keys()
    expired_clusters = set()
    for cluster_name in current_clusters:
        try:
            old_nodes = json.loads(cluster_name)
            nodes_list = [[primary] + nodes for primary, nodes in agents.items()]
            if not any([set(nodes) & set(old_nodes) for nodes in nodes_list]):
                expired_clusters.add(cluster_name)

        except json.decoder.JSONDecodeError:
            if cluster_name in meta_detectors:
                delete_ids = [detector[DetectorParam.DETECTOR_ID]
                              for detector in meta_detectors[cluster_name].values()]
                delete_anomaly_detectors(delete_ids)

    for cluster_name in expired_clusters:
        nodes = json.loads(cluster_name)
        result = clear_detector(nodes)
        if result.split(':')[0] == 'Success':
            logging.info("AD_PoolManager: Successfully clear expired cluster %s.", cluster_name)


def get_detector_init_defaults():
    drop_down = {
        "side": ["positive", "negative", "both"],
        "agg": ['median', 'mean', 'std'],
    }

    ranges = {
        "percentage": [0, 1],
    }

    detector_info = {
        "AlarmInfo": {
            "alarm_content": None,
            "alarm_type": (
                ALARM_TYPES.SYSTEM,
                [k for k in ALARM_TYPES.__dict__ if not k.endswith("__")]
            ),
            "alarm_level": (
                ALARM_LEVEL.ERROR.name,
                [k for k in ALARM_LEVEL.__members__]
            ),
            "alarm_cause": None,
            "extra": None
        }
    }

    for detector_name, detector in detectors.items():
        detector_info[detector_name] = dict()
        init_arg = inspect.getfullargspec(detector.__init__)
        args, defaults = init_arg.args[1:], init_arg.defaults
        for i, arg in enumerate(args):
            if arg in drop_down:
                detector_info[detector_name][arg] = (defaults[i], drop_down.get(arg, []))
            elif arg in ranges:
                detector_info[detector_name][arg] = (defaults[i], ranges.get(arg, []))
            elif defaults[i] in (float("inf"), -float("inf")):
                detector_info[detector_name][arg] = None
            else:
                detector_info[detector_name][arg] = defaults[i]

    return detector_info


def init_specific_detections(primary, nodes):
    cluster_name = json.dumps(sorted(nodes))
    meta_detectors = get_detectors_from_metadatabase(nodes)
    recorded_detectors = meta_detectors.get(cluster_name, {})
    recorded_detector_names = list(recorded_detectors.keys())
    for meta_detector in recorded_detectors.values():
        detector_name = meta_detector[DetectorParam.DETECTOR_NAME]
        meta_dict = meta_detector.copy()
        instance_set = get_specific_agents(split_ip_port(primary)[0])
        instance = '|'.join([f"{prepare_ip(ip)}{PORT_SUFFIX}|{ip}" for ip in instance_set])
        detector_info = json.loads(meta_dict[DetectorParam.DETECTOR_INFO])
        detector_info[0]["metric_filter"]["from_instance"] = instance
        meta_dict[DetectorParam.DETECTOR_INFO] = json.dumps(detector_info)
        add_detectors_to_metadatabase(nodes, detector_name, meta_dict)

    for detector_name, json_dict in SpecificDetection.detections.items():
        if detector_name not in recorded_detector_names:
            add_detector(primary, nodes, detector_name, json_dict, fuzzy_match=True)


def parsing_json_kwarg(json_dict):
    try:
        alarm_info_dict = {
            k: json_dict[DetectorParam.ALARM_INFO][k] for k in
            (json_dict[DetectorParam.ALARM_INFO].keys() & ALARM_INFO_ARGS)
        }
        alarm_info = AlarmInfo(**alarm_info_dict)

        detector_info = list()
        for arg_dict in json_dict[DetectorParam.DETECTOR_INFO]:
            detector_info_dict = {
                k: arg_dict[k] for k in
                (arg_dict.keys() & DETECTOR_INFO_ARGS)
            }
            detector_info.append(detector_info_dict)

        return (json_dict[DetectorParam.DURATION],
                json_dict[DetectorParam.FORECASTING_SECONDS],
                alarm_info,
                detector_info)

    except KeyError:
        logging.warning("AD_PoolManager: detector info lacks attributes.")
        return None


def meta_dict_to_cached_detactor(meta_dict):
    alarm_info = AlarmInfo(
        alarm_content=meta_dict[DetectorParam.ALARM_CONTENT],
        alarm_type=meta_dict[DetectorParam.ALARM_TYPE],
        alarm_level=meta_dict[DetectorParam.ALARM_LEVEL],
        alarm_cause=meta_dict[DetectorParam.ALARM_CAUSE],
        extra=meta_dict[DetectorParam.EXTRA]
    )

    detector_info = list()
    for arg_dict in json.loads(meta_dict[DetectorParam.DETECTOR_INFO]):
        detector_info_dict = {
            k: arg_dict[k] for k in
            (arg_dict.keys() & DETECTOR_INFO_ARGS)
        }
        detector_info.append(DetectorInfo(**detector_info_dict))

    return {
        DetectorParam.RUNNING: meta_dict[DetectorParam.RUNNING],
        DetectorParam.DETECTOR: GenericAnomalyDetector(
            meta_dict[DetectorParam.DETECTOR_NAME],
            meta_dict[DetectorParam.DURATION],
            meta_dict[DetectorParam.FORECASTING_SECONDS],
            alarm_info,
            detector_info
        )
    }


def meta_dict_to_proc_dict(meta_dict):
    """Convert meta detector dict to processed structure."""
    try:
        return {
            DetectorParam.RUNNING: meta_dict[DetectorParam.RUNNING],
            DetectorParam.DURATION: meta_dict[DetectorParam.DURATION],
            DetectorParam.FORECASTING_SECONDS: meta_dict[DetectorParam.FORECASTING_SECONDS],
            DetectorParam.ALARM_INFO: {
                DetectorParam.ALARM_CAUSE: meta_dict[DetectorParam.ALARM_CAUSE],
                DetectorParam.ALARM_CONTENT: meta_dict[DetectorParam.ALARM_CONTENT],
                DetectorParam.ALARM_LEVEL: meta_dict[DetectorParam.ALARM_LEVEL],
                DetectorParam.ALARM_TYPE: meta_dict[DetectorParam.ALARM_TYPE]
            },
            DetectorParam.DETECTOR_INFO: json.loads(meta_dict[DetectorParam.DETECTOR_INFO])
        }
    except (KeyError, AttributeError):
        logging.warning("Detector may be broken: %s", meta_dict)
        return {}


def rebuild_detector(nodes):
    logging.info("AD_PoolManager: Successfully rebuild detectors for %s.", nodes)
    return f"Success: rebuild detectors for {nodes}."
