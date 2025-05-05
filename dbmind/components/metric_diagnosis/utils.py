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

import json
import logging
from datetime import datetime

from dbmind import global_vars
from dbmind.app.monitoring.ad_pool_manager import get_detectors_from_metadatabase
from dbmind.app.monitoring.monitoring_constants import DetectorParam
from dbmind.app.monitoring.specific_detection import SpecificDetection
from dbmind.common.exceptions import DontIgnoreThisError
from dbmind.service import dai


def get_metric(metric_name, start_time, end_time, step=None,
               labels=None, labels_like=None, fetch_all=True,
               min_length=2):
    """the metric scraping method"""

    def check_seq_length(sequence, minimal_length):
        """raise ValueError when sequence scraped is too short."""
        if len(sequence) < minimal_length:
            raise DontIgnoreThisError(
                "No sequence fetched for metric '%s'. Possible reasons: "
                "1. Check the metric-filter: %s; "
                "2. There is not enough data points from %s to %s."
                % (metric_name, {**labels, **labels_like},
                   datetime.fromtimestamp(start_time),
                   datetime.fromtimestamp(end_time))
            )

    if not isinstance(labels, dict):
        labels = {}

    if not isinstance(labels_like, dict):
        labels_like = {}

    fetcher = dai.get_metric_sequence(
        metric_name,
        datetime.fromtimestamp(start_time),
        datetime.fromtimestamp(end_time),
        step=step
    ).filter(**labels).filter_like(**labels_like)

    if fetch_all:
        seqs = fetcher.fetchall()
        for seq in seqs:
            check_seq_length(seq, min_length)

        return seqs

    seq = fetcher.fetchone()
    check_seq_length(seq, min_length)

    return seq


def get_detector_params(detector_name, param, default=None):
    if default is None:
        detector_info = SpecificDetection.detections[detector_name][DetectorParam.DETECTOR_INFO]
        default = detector_info[0]["detector_kwargs"][param]

    try:
        primary = global_vars.agent_proxy.current_agent_addr()
        nodes = global_vars.agent_proxy.agent_get_all().get(primary, [])
        if primary not in nodes:
            nodes.append(primary)
    except AttributeError:
        return default

    try:
        cluster_name = json.dumps(sorted(nodes))
        meta_detectors = get_detectors_from_metadatabase(nodes, detector_name)
        if not meta_detectors.get(cluster_name):
            return default
        else:
            detector_info = meta_detectors[cluster_name][detector_name][DetectorParam.DETECTOR_INFO]
            detector_info = json.loads(detector_info)
            return detector_info[0]["detector_kwargs"][param]
    except Exception as e:
        logging.exception(e)
        return default
