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

import multiprocessing
import threading

from dbmind.common.dispatcher.task_worker import get_mp_sync_manager
from dbmind.common.platform import LINUX

from .evt.spot import BiSPOT, BiDSPOT, ESPOT
from ._abstract_detector import AbstractDetector
from ...types import Sequence

lock = threading.Lock()
if LINUX:
    sync_manager = get_mp_sync_manager()
    try:
        sync_manager.start()
    except multiprocessing.context.ProcessError:
        pass

    evt_detectors_dict = sync_manager.dict()
else:
    evt_detectors_dict = None

DETECTORS = {
    "bispot": BiSPOT,
    "bidspot": BiDSPOT,
    "espot": ESPOT
}


class EvtDetector(AbstractDetector):
    """
    This class allows to run SPOT algorithm on univariate dataset (upper and lower bounds)

    Attributes
    ----------
    probability : float
        Detection level (risk), chosen by the user

    depth : int
        Number of observations to compute the moving average

    update_interval : int
        retrain interval

    method : str
        SPOT models choices from ("bispot", "bidspot", "espot")

    """

    def __init__(self, probability=1e-4, depth=40, update_interval=0, side="both", method="bispot"):
        self.probability = probability
        self.depth = depth
        self.update_interval = update_interval
        self.side = side
        self.method = method
        self.evt_detectors_dict = evt_detectors_dict

    def _fit(self, s: Sequence) -> None:
        metric_key = (
            (s.name, self.probability, self.depth, self.update_interval, self.method) +
            tuple(sorted(s.labels.items()))
        )
        with lock:
            if metric_key not in self.evt_detectors_dict:
                evt_detector = DETECTORS[self.method](
                    probability=self.probability,
                    depth=self.depth,
                    update_interval=self.update_interval,
                    side=self.side
                )
                evt_detector.fit(s.values)
                self.evt_detectors_dict[metric_key] = evt_detector

    def _predict(self, s: Sequence) -> Sequence:
        metric_key = (
            (s.name, self.probability, self.depth, self.update_interval, self.method) +
            tuple(sorted(s.labels.items()))
        )
        with lock:
            evt_detector = self.evt_detectors_dict[metric_key]

        res = evt_detector.predict(s)
        return Sequence(timestamps=s.timestamps[-len(res):], values=res)
