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
threshold settings about self_security metrics
"""
import logging
from dbmind.common.utils import cast_to_int_or_float
from dbmind.app.monitoring import get_dynamic_param


SECURITY_METRICS_SETTINGS = {
    "on": {"type": int, "values": [0, 1], "default": 0},
    "model_min_forecast_length_in_minute": {"type": int, "min": 5, "max": 60, "default": 15},
    "save_model_outputs": {"type": int, "values": [0, 1, 2], "default": 0},
    "save_model_outputs_disk_size_in_kb": {"type": int, "min": 0, "max": 1000000, "default": 10000},
    "calibration_day_of_week": {"type": int, "min": -1, "max": 6, "default": -1},
    "calibration_hour": {"type": int, "min": -1, "max": 23, "default": -1},
    "calibration_training_in_minute": {"type": int, "min": 10080, "max": 40320, "default": 20160},
    "calibration_forecasting_in_minutes": {"type": int, "min": 1440, "max": 2880, "default": 1440},
    "re_calibrate_period": {"type": int, "min": 5040, "max": float("inf"), "default": 10080},
    "detection_training_in_minutes": {"type": int, "min": 1440, "max": 2880, "default": 1440},
    "detection_forecasting_in_minutes": {"type": int, "min": 30, "max": 60, "default": 30},
    "scenario_high_alert": {"type": float, "min": 0, "max": 1, "default": 0.8},
    "scenario_medium_alert": {"type": float, "min": 0, "max": 1, "default": 0.6},
    "scenario_low_alert": {"type": float, "min": 0, "max": 1, "default": 0.2},
    "ratio_conf_factor": {"type": float, "min": 0, "max": 100, "default": 3.5},
    "z_score_conf_factor": {"type": float, "min": 0, "max": 100, "default": 3.5},
    "opengauss_invalid_logins_rate_lower_bound": {"type": float, "min": 0, "max": float("inf"), "default": 2.0},
    "opengauss_log_errors_rate_lower_bound": {"type": float, "min": 0, "max": float("inf"), "default": 2.0},
    "opengauss_user_violation_rate_lower_bound": {"type": float, "min": 0, "max": float("inf"), "default": 2.0},
    "opengauss_user_locked_rate_lower_bound": {"type": float, "min": 0, "max": float("inf"), "default": 2.0}
}


def get_security_metrics_settings(name: str):
    """
    gets security metrics dynamic settings
    @param name: setting name
    @return the setting value, int or float
    """
    value = get_dynamic_param('security_metrics', name)
    value = cast_to_int_or_float(value)
    logging.info("get_security_metrics_settings %s - %s", name, value)

    try:
        if "values" in SECURITY_METRICS_SETTINGS.get(name, dict()):
            values = SECURITY_METRICS_SETTINGS.get(name).get('values', list())
            if value not in values:
                default = SECURITY_METRICS_SETTINGS.get(name).get('default', 0)
                logging.warning("security metrics setting of %s value %s is not in %s, using the default of %s",
                                name, value, values, default)
                value = default
        elif "min" in SECURITY_METRICS_SETTINGS.get(name) and "max" in SECURITY_METRICS_SETTINGS.get(name):
            min_value = SECURITY_METRICS_SETTINGS.get(name).get('min')
            max_value = SECURITY_METRICS_SETTINGS.get(name).get('max')
            if value < min_value or value > max_value:
                default = SECURITY_METRICS_SETTINGS.get(name).get('default', 0)
                logging.warning("security metrics setting of %s value %s is not between %s and %s "
                                "using the default of %s", name, value, min_value, max_value, default)
                value = default
    except (TypeError, KeyError) as _:
        #  may happen for lower bound for metrics, just skip this
        if value != value:  # means value is float('nan')
            return 0
        return value

    return value
