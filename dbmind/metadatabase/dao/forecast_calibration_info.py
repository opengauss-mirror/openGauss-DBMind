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
import datetime
import logging

from dbmind.common.algorithm.anomaly_detection.forecasting_anomaly_detector import ForecastingAnomalyDetector
from dbmind.metadatabase.result_db_session import get_session
from dbmind.metadatabase.schema.security_metric_models import SecurityMetricModels


class ModelCalibrationData:
    """
    saves the calibration data to the database after calibration is done
    loads the calibration data from the database before using the values for anomaly detection
    """
    PARAMETER_KEYS = ["max_z_score", "std_errors", "avg_errors", "max_ratio", "step"]
    MAX_SCORE_KEY = 0
    STD_ERROR_KEY = 1
    AVG_ERROR_KEY = 2
    MAX_RATIO_KEY = 3
    STEP_KEY = 4

    def __init__(self):
        pass

    def save_to_database(self, metric_name: str, host: str, calibration_model: ForecastingAnomalyDetector, step: int):
        """
        read metric calibration data from the calibration model and saves it into the database
        @param metric_name: the metric to save the parameters for
        @param host: the host name
        @param calibration_model: the model that was used for calibration
        @return: n/a
        """
        data = [(self.PARAMETER_KEYS[self.MAX_SCORE_KEY], calibration_model.get_max_z_score()),
                (self.PARAMETER_KEYS[self.STD_ERROR_KEY], calibration_model.get_std_errors()),
                (self.PARAMETER_KEYS[self.AVG_ERROR_KEY], calibration_model.get_avg_errors()),
                (self.PARAMETER_KEYS[self.MAX_RATIO_KEY], calibration_model.get_max_ratio()),
                (self.PARAMETER_KEYS[self.STEP_KEY], step)]
        with get_session() as session:
            # delete the previous parameters
            query = session.query(SecurityMetricModels)
            _ = query.filter((SecurityMetricModels.metric_name == metric_name),
                             (SecurityMetricModels.host == host)).delete()
            # add the new parameters
            for item in data:
                session.add(
                    SecurityMetricModels(
                        host=host,
                        metric_name=metric_name,
                        parameter_name=item[0],
                        parameter_value=str(item[1]),
                        time_saved=calibration_ts()
                    )
                )

    def load_from_database(self, metric_name: str, host: str):
        """
        loads metric calibration data from the database
        @param metric_name: the metric to save the parameters for
        @param host: host name
        @return: dictionary that can be later be used as key word arguments on the model
        """
        data = dict()
        did_get_data = False
        with get_session() as session:
            for key in self.PARAMETER_KEYS:
                data[key] = ""
            query = session.query(SecurityMetricModels).filter((SecurityMetricModels.metric_name == metric_name),
                                                               (SecurityMetricModels.host == host))
            for record in query:
                if record.parameter_name not in ["algorithm_name"]:
                    value = float(record.parameter_value)
                    did_get_data = True
                    key = record.parameter_name
                    data[key] = value
        if not did_get_data:
            return None
        logging.debug("Load from DB: %s", data)
        return data


def calibration_ts():
    ct = datetime.datetime.now()
    ts = ct.timestamp()
    return int(ts)
