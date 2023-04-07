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
import logging
import os
from typing import Union, Iterable

import yaml

from dbmind.app.monitoring.specific_detection import SpecificDetection
from dbmind.app.monitoring.generic_anomaly_detector import (
    get_monitoring_alarm_args,
    get_monitoring_detector_args,
    AlarmInfo,
    DetectorInfo,
    GenericAnomalyDetector
)
from dbmind.common.algorithm.anomaly_detection import detectors
from dbmind.common.types import ALARM_TYPES, ALARM_LEVEL

YAML_PATH = os.path.join(os.path.dirname(__file__), "detectors.yml")

ALARM_INFO_ARGS = get_monitoring_alarm_args()
DETECTOR_INFO_ARGS = get_monitoring_detector_args()

_anomaly_detector_pool = dict()


class DetectorAction:
    PAUSE = "pause"
    RESUME = "resume"


class DetectorParam:
    DETECTOR = "detector"
    RUNNING = "running"
    DURATION = "duration"
    FORECASTING_SECONDS = "forecasting_seconds"
    ALARM_INFO = "alarm_info"
    DETECTOR_INFO = "detector_info"


def get_anomaly_detectors():
    return _anomaly_detector_pool.values()


def add_detector(name, json_dict):
    arguments = parsing_json_kwarg(json_dict)
    proc_dict = preprocessing_dict(json_dict)
    proc_dict[DetectorParam.RUNNING] = 1
    try:
        duration, forecasting_seconds, alarm_info, detector_info = arguments
        _add_detector(
            name,
            duration,
            forecasting_seconds,
            alarm_info,
            detector_info
        )
        add_yaml({name: proc_dict})
        logging.info(f"AD_PoolManager: Successfully add detector {name}.")
        return f"Success: add {name}"
    except Exception as e:
        logging.error(e)
        logging.info("AD_PoolManager: No new detectors added.")
        return f"Failed: add {name}"


def _add_detector(
        name: str,
        duration: int,
        forecasting_seconds: int,
        alarm_info: AlarmInfo,
        detector_info: Union[DetectorInfo, Iterable[DetectorInfo]]
):
    _anomaly_detector_pool[name] = {
        DetectorParam.RUNNING: 1,
        DetectorParam.DETECTOR: GenericAnomalyDetector(
            name,
            duration,
            forecasting_seconds,
            alarm_info,
            detector_info
        )
    }


def delete_detector(name):
    try:
        _anomaly_detector_pool.pop(name)
        delete_yaml(name)
        logging.info(f"AD_PoolManager: Successfully delete detector {name}.")
        return f"Success: delete {name}"
    except KeyError:
        logging.info(f"AD_PoolManager: Detector {name} is not found.")
        return f"Failed: delete {name}"
    except Exception as e:
        logging.error(e)
        return f"Failed: delete {name}"


def _adjust_detector(name, action):
    running = 0 if action == "pause" else 1
    try:
        _anomaly_detector_pool[name][DetectorParam.RUNNING] = running
        yaml_obj = read_yaml(YAML_PATH)
        yaml_obj[name][DetectorParam.RUNNING] = running
        add_yaml(yaml_obj)
        logging.info(f"AD_PoolManager: Successfully {action} detector {name}.")
        return f"Success: {action} {name}"
    except KeyError:
        logging.info(f"AD_PoolManager: Detector {name} is not found.")
        return f"Failed: {action} {name}"
    except Exception as e:
        logging.error(e)
        return f"Failed: {action} {name}"


def pause_detector(name):
    return _adjust_detector(name, DetectorAction.PAUSE)


def resume_detector(name):
    return _adjust_detector(name, DetectorAction.RESUME)


def view_detector(name):
    if name == "all":
        return read_yaml(YAML_PATH)
    else:
        return read_yaml(YAML_PATH).get(name, {})


def clear_detector():
    try:
        _anomaly_detector_pool.clear()
        write_yaml(YAML_PATH, {})
        return f"Success: clear detectors"
    except Exception as e:
        logging.error(e)
        return f"Failed: clear detectors"


def rebuild_detector():
    try:
        yaml_obj = read_yaml(YAML_PATH)
        for name, params in yaml_obj.items():
            running = params[DetectorParam.RUNNING]
            duration = params[DetectorParam.DURATION]
            forecasting_seconds = params[DetectorParam.FORECASTING_SECONDS]
            alarm_info = AlarmInfo(**params[DetectorParam.ALARM_INFO])
            detector_info = list()
            for arg_dict in params[DetectorParam.DETECTOR_INFO]:
                detector_info.append(DetectorInfo(**arg_dict))
            _anomaly_detector_pool[name] = {
                    DetectorParam.RUNNING: running,
                    DetectorParam.DETECTOR: GenericAnomalyDetector(
                        name, duration, forecasting_seconds, alarm_info, detector_info
                    )
            }
        return f"Success: rebuild detectors"
    except Exception as e:
        logging.error(e)
        return f"Failed: rebuild detectors"


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


def init_specific_detections():
    for name, json_dict in SpecificDetection.detections.items():
        if name not in _anomaly_detector_pool:
            add_detector(name, json_dict)


def write_yaml(yaml_path, yaml_obj):
    yaml_obj = {name: preprocessing_dict(params)
                for name, params in yaml_obj.items()}
    with open(yaml_path, "w+") as f:
        yaml.dump(yaml_obj, f)


def read_yaml(yaml_path):
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            yaml_obj = yaml.safe_load(f.read())
        yaml_obj = {name: preprocessing_dict(params)
                    for name, params in yaml_obj.items()}
    except FileNotFoundError:
        write_yaml(yaml_path, {})
        yaml_obj = read_yaml(yaml_path)

    return yaml_obj


def delete_yaml(name):
    yaml_obj = read_yaml(YAML_PATH)
    try:
        yaml_obj.pop(name)
    except KeyError:
        logging.info(f"AD_PoolManager: Detector {name} is not found in local yaml.")

    write_yaml(YAML_PATH, yaml_obj)


def add_yaml(new_yaml_obj):
    yaml_obj = read_yaml(YAML_PATH)
    yaml_obj.update(new_yaml_obj)
    write_yaml(YAML_PATH, yaml_obj)


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
            detector_info.append(DetectorInfo(**detector_info_dict))

        return (json_dict[DetectorParam.DURATION],
                json_dict[DetectorParam.FORECASTING_SECONDS],
                alarm_info,
                detector_info)

    except KeyError:
        logging.warning(f"AD_PoolManager: detector info lacks attributes.")
        return None


def preprocessing_dict(json_dict):
    proc_dict = dict()
    try:
        proc_dict[DetectorParam.RUNNING] = json_dict.get(DetectorParam.RUNNING, 1)
        proc_dict[DetectorParam.DURATION] = json_dict.get(DetectorParam.DURATION, 180)
        proc_dict[DetectorParam.FORECASTING_SECONDS] = \
            json_dict.get(DetectorParam.FORECASTING_SECONDS, 0)

        alarm_info_dict = {
            k: json_dict[DetectorParam.ALARM_INFO][k] for k in
            (json_dict[DetectorParam.ALARM_INFO].keys() & ALARM_INFO_ARGS)
        }
        proc_dict[DetectorParam.ALARM_INFO] = alarm_info_dict

        proc_dict[DetectorParam.DETECTOR_INFO] = list()
        for arg_dict in json_dict[DetectorParam.DETECTOR_INFO]:
            detector_info_dict = {
                k: arg_dict[k] for k in
                (arg_dict.keys() & DETECTOR_INFO_ARGS)
            }
            proc_dict[DetectorParam.DETECTOR_INFO].append(detector_info_dict)

    except KeyError:
        logging.warning(f"AD_PoolManager: detector info lacks attributes.")
        return {}

    return proc_dict
