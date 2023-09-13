import csv
import numpy as np

from sklearn.preprocessing import MinMaxScaler


class Knobs:
    def __init__(self, knobs_csv_path, candidates=[]) -> None:
        # knobs name upper lower bound file
        self.knobs_csv_path = knobs_csv_path
        with open(knobs_csv_path, "r") as f:
            reader = csv.DictReader(f)
            self.knobs_info = [row for row in reader]
        self.all_knobs_range = {}
        self.all_knobs_default = {}
        self.enum_choices = []
        self.enum_turn_to_int = {"off": 0, "on": 1}
        self.candidates = candidates
        # knob range and enum choices for turn enum to int
        for row in self.knobs_info:
            knob_name = row["name"]
            if row["category"] in ["int", "float"]:
                self.all_knobs_range[knob_name] = [
                    row["lower_bound"],
                    row["upper_bound"],
                ]
                self.all_knobs_default[knob_name] = row["default"]
            else:
                choices = row["enum_choices"].strip("[]").split(",")
                standard_choices = [choice.strip().strip("'") for choice in choices]
                self.enum_choices.append(standard_choices)
                self.all_knobs_range[knob_name] = [0, len(choices) - 1]
        self.enum_choices.sort(key=len)

    @property
    def candidates(self):
        return self._candidates

    @candidates.setter
    def candidates(self, value):
        self._candidates = value
        knobs = [ki["name"] for ki in self.knobs_info]
        if not all([cand in knobs for cand in self._candidates]):
            raise ValueError("eval knobs not found")
        self.candidates_info = [
            ki for ki in self.knobs_info if ki["name"] in self._candidates
        ]

    def knob_normalization(self, knob_data, knobs):
        min_values = [self.all_knobs_range[knob][0] for knob in knobs]
        max_values = [self.all_knobs_range[knob][1] for knob in knobs]
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaler.fit(np.array([min_values, max_values]))
        return scaler.transform(knob_data)

    def enum_turn(self, knob_data):
        for choices in self.enum_choices:
            value = 0
            ban_value = {
                choice: self.enum_turn_to_int[choice]
                for choice in choices
                if choice in self.enum_turn_to_int.keys()
            }
            for choice in choices:
                if choice not in ban_value.keys():
                    while value in ban_value.values():
                        value = value + 1
                    self.enum_turn_to_int[choice] = value
                    value = value + 1

        def replace_func(value):
            return self.enum_turn_to_int.get(value, value)

        v_func = np.vectorize(replace_func)
        return v_func(knob_data)

    def knob_align(self, src: list, target: list):
        res = []
        for s in src:
            X = []
            for tb in target:
                if tb not in s.keys():
                    X.append(self.all_knobs_default[tb])
                else:
                    X.append(s[tb])
            res.append(X)
        res = self.enum_turn(np.array(res))
        return res.astype(np.float64)


def check_config(configuration: dict):
    rules = []

    shared_buffers = configuration.get("shared_buffers", 0)
    max_connections = configuration.get("max_connections", 0)
    work_mem = configuration.get("work_mem", 0)
    effective_cache_size = configuration.get("effective_cache_size", 0)
    effective_io_concurrency = configuration.get("effective_io_concurrency", 0)
    max_parallel_workers = configuration.get("max_parallel_workers", 0)
    random_page_cost = configuration.get("random_page_cost", 0)
    seq_page_cost = configuration.get("seq_page_cost", 0)
    checkpoint_completion_target = configuration.get("checkpoint_completion_target", 0)
    checkpoint_timeout = configuration.get("checkpoint_timeout", 0)
    wal_writer_delay = configuration.get("wal_writer_delay", 0)
    max_wal_size = configuration.get("max_wal_size", 0)

    if shared_buffers + (max_connections * work_mem) > effective_cache_size:
        rules.append(
            "Rule 1: shared_buffers + (max_connections * work_mem) ≤ effective_cache_size"
        )

    if shared_buffers <= effective_io_concurrency * max_parallel_workers:
        rules.append(
            "Rule 2: shared_buffers > effective_io_concurrency * max_parallel_workers"
        )

    if effective_io_concurrency * random_page_cost <= seq_page_cost:
        rules.append(
            "Rule 3: effective_io_concurrency * random_page_cost > seq_page_cost"
        )

    if (
        checkpoint_completion_target * checkpoint_timeout
        <= wal_writer_delay * max_wal_size
    ):
        rules.append(
            "Rule 4: checkpoint_completion_target * checkpoint_timeout > wal_writer_delay * max_wal_size"
        )

    if seq_page_cost < random_page_cost / 1.5:
        rules.append("Rule 5: seq_page_cost ≥ random_page_cost / 1.5")

    if len(rules) != 0:
        return "不满足规则：\n" + "\n".join(rules)
    return None
