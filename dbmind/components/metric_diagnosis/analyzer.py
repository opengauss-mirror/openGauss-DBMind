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
"""The Analyzer"""

import logging
from datetime import datetime

try:
    from scipy.interpolate import interp1d
except ImportError:
    pass

from dbmind.common.algorithm import anomaly_detection
from dbmind.common.algorithm.anomaly_detection.agg import merge_with_and_operator
from dbmind.common.algorithm.correlation import CorrelationAnalysis
from dbmind.common.exceptions import DontIgnoreThisError
from dbmind.common.types import Sequence
from dbmind.components.metric_diagnosis.utils import get_metric


class Analyzer:
    """The Abstract Analyzer"""
    def __init__(self, analyzer_args):
        self.metric_name = analyzer_args.metric_name
        self.metric_filter = analyzer_args.metric_filter
        self.metric_filter_like = analyzer_args.metric_filter_like
        self.length = analyzer_args.length
        self.params = analyzer_args.params
        if isinstance(self.params, dict):
            for name, param in self.params.items():
                if callable(param):
                    self.params[name] = param(name)

        self.score = analyzer_args.score
        self.step = analyzer_args.step
        self.related_seqs = {"normal": [], "abnormal": []}
        self.record = bool(analyzer_args.record)
        if analyzer_args.mode == "beginning":
            self.main_metric_sequence = analyzer_args.beginning_main_seq
            if self.length:
                self.end = max(analyzer_args.beginning_start + self.length, analyzer_args.beginning_end)
            else:
                self.end = analyzer_args.beginning_end

            self.start = analyzer_args.beginning_start
        else:
            self.main_metric_sequence = analyzer_args.recent_main_seq
            if self.length:
                self.start = min(analyzer_args.recent_end - self.length, analyzer_args.recent_start)
            else:
                self.start = analyzer_args.recent_start

            self.end = analyzer_args.recent_end

    def query_range(self, min_length=2):
        seqs = get_metric(
            self.metric_name,
            self.start,
            self.end,
            step=self.step,
            labels=self.metric_filter,
            labels_like=self.metric_filter_like,
            fetch_all=True,
            min_length=min_length
        )

        if not seqs:
            logging.info("Missing target sequence: %s %s %s from %s to %s.",
                         self.metric_name, self.metric_filter,
                         self.metric_filter_like,
                         datetime.fromtimestamp(self.start),
                         datetime.fromtimestamp(self.end))

        return seqs

    def record_seq(self, seq, abnormal):
        """To decide whether to record the related sequence"""
        if not self.record:
            return

        if abnormal:
            self.related_seqs["abnormal"].append(seq)
        else:
            self.related_seqs["normal"].append(seq)

    def detect(self, detector, seqs):
        res = list()
        for seq in seqs:
            ans = detector.fit_predict(seq)
            if True in ans.values:
                res.append(self.score)
                self.record_seq(seq, True)
            else:
                res.append(0)
                self.record_seq(seq, False)

        if not res:
            return 0

        return max(res)


class Correlation(Analyzer):
    """The Correlation Analyzer"""
    def analyze(self):
        if self.params is None:
            self.params = {"negative": -0.3, "positive": 0.3, "preprocess_method": "diff"}

        try:
            seqs = self.query_range()
        except DontIgnoreThisError:
            seqs = list()

        res = list()
        for seq in seqs:
            interpolation_func = interp1d(
                seq.timestamps,
                seq.values,
                kind='linear',
                bounds_error=False,
                fill_value=(seq.values[0], seq.values[-1])
            )
            interpolated_y = interpolation_func(self.main_metric_sequence.timestamps)
            analyzer = CorrelationAnalysis(analyze_method='pearson',
                                           preprocess_method=self.params["preprocess_method"])
            seq_1, seq_2 = analyzer.preprocess(self.main_metric_sequence.values, interpolated_y)
            corr, _ = analyzer.analyze(seq_1, seq_2)
            positive = False if self.params["positive"] is None else corr > self.params["positive"]
            negative = False if self.params["negative"] is None else corr < self.params["negative"]
            if positive or negative:
                res.append(self.score or abs(corr))
                self.record_seq(seq, True)
            else:
                res.append(0)
                self.record_seq(seq, False)

        if not res:
            return 0

        return max(res)


class Increase(Analyzer):
    """The Increase Analyzer"""
    def analyze(self):
        if self.score is None:
            self.score = 1

        seqs = self.query_range()
        detector = anomaly_detection.IncreaseDetector(side="positive")
        if self.params is not None:
            grad_detector = anomaly_detection.GradientDetector(
                side="positive",
                max_coef=self.params["max_coef"]
            )
            res = list()
            for seq in seqs:
                ans = merge_with_and_operator([
                    detector.fit_predict(seq),
                    grad_detector.fit_predict(seq)
                ])
                if True in ans.values:
                    res.append(self.score)
                    self.record_seq(seq, True)
                else:
                    res.append(0)
                    self.record_seq(seq, False)

            if not res:
                return 0

            return max(res)

        return self.detect(detector, seqs)


class Threshold(Analyzer):
    """The Threshold Analyzer"""
    def analyze(self):
        if self.params is None:
            raise ValueError("Threshold Analyzer doesn't have params.")

        if self.score is None:
            self.score = 1

        seqs = self.query_range(min_length=1)
        detector = anomaly_detection.ThresholdDetector(
            high=self.params["high"],
            low=self.params["low"],
            percentage=self.params.get("percentage"),
            closed=self.params.get("closed")
        )
        return self.detect(detector, seqs)


class LevelShift(Analyzer):
    """The Level Shift Analyzer"""
    def analyze(self):
        if self.params is None:
            self.params = {"upper_outliers": 6, "side": "positive", "window": 5}

        if self.score is None:
            self.score = 1

        seqs = self.query_range()
        detector = anomaly_detection.LevelShiftDetector(
            outliers=(None, self.params["upper_outliers"]),
            side=self.params["side"],
            window=self.params["window"],
            agg="mean"
        )
        return self.detect(detector, seqs)


class MeanThreshold(Analyzer):
    """The Average of Sequence Threshold Analyzer"""
    def analyze(self):
        if self.params is None:
            raise ValueError("Threshold Analyzer doesn't have params.")

        if self.score is None:
            self.score = 1

        seqs = self.query_range(min_length=1)
        detector = anomaly_detection.ThresholdDetector(
            high=self.params["high"],
            low=self.params["low"]
        )
        res = list()
        for seq in seqs:
            mean_seq = Sequence(
                timestamps=[seq.timestamps[-1]],
                values=[sum(seq.values) / (seq.timestamps[-1] - seq.timestamps[0] + self.step)]
            )
            ans = detector.fit_predict(mean_seq)
            if True in ans.values:
                res.append(self.score)
                self.record_seq(seq, True)
            else:
                res.append(0)
                self.record_seq(seq, False)

        if not res:
            return 0

        return max(res)


class Empty(Analyzer):
    """The Empty Sequences Analyzer"""
    def analyze(self):
        if self.score is None:
            self.score = 1

        seqs = self.query_range(min_length=1)
        if not seqs:
            return self.score

        return 0
