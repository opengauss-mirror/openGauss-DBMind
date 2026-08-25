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
from typing import Optional

from dbmind.common.algorithm.basic import binary_search, binary_search_leftmost, binary_search_rightmost
from dbmind.common.algorithm.basic import how_many_lesser_elements
from dbmind.common.utils import dbmind_assert
from dbmind.common.utils.checking import uniform_labels
from ..either import OptionalContainer, OptionalValue
from ..rpc import RPCJSONAble
from ..utils import cached_property

_EMPTY_TUPLE = tuple()


class Sequence(RPCJSONAble):
    def jsonify(self):
        return {
            'name': self.name,
            'timestamps': self.timestamps,
            'values': self.values,
            'labels': self.labels,
        }

    @classmethod
    def get_instance(cls, data):
        return cls(
            data['timestamps'], data['values'], data['name'], labels=data['labels']
        )

    @staticmethod
    def _align_timestamps(raw: tuple, step: int):
        if len(raw) == 0:
            return raw

        diff = [0 for _ in range(len(raw))]
        diff[0] = raw[0]
        for i in range(1, len(raw)):
            diff[i] = raw[i] - raw[i - 1]

        for i in range(1, len(diff)):
            if diff[i] % step > 0:
                if diff[i] // step == 0:
                    diff[i] = step
                else:
                    diff[i] -= (diff[i] % step)
        rv = list(raw)
        for i in range(1, len(diff)):
            rv[i] = rv[i - 1] + diff[i]
        return tuple(rv)

    def __init__(self, timestamps=None, values=None, name=None, step=None, labels=None, align_timestamp=False):
        """Sequence is an **immutable** data structure, which wraps time series and
        its information.

        .. attention::

            All properties are cached property due to immutability.
            Forced to modify a Sequence object could cause error.

        It is one and only representation for time series in DBMind."""
        timestamps = OptionalValue(timestamps).get(_EMPTY_TUPLE)
        values = OptionalValue(values).get(_EMPTY_TUPLE)
        self._timestamps = tuple(timestamps)
        self._values = tuple(values)
        # ``self.name`` is an optional variable.
        self.name = name
        self._step = step
        self._labels = labels or {}
        # Attach sub-sequence to parent-sequence with logical pointer.
        # By this means, sub-sequence can avoid redundant records.
        self._parent = None
        self._parent_start = None
        self._parent_end = None

        if align_timestamp:
            self._timestamps = self._align_timestamps(self._timestamps, self.step)
        success, message = Sequence._check_validity(self._timestamps, self._values)
        if not success:
            raise ValueError(message)

    def set_name(self, name: str):
        self.name = name

    @staticmethod
    def _check_validity(timestamps, values):
        # invalid scenarios
        if None in (timestamps, values):
            return False, 'NoneType object is not iterable.'
        if len(timestamps) != len(values):
            return False, 'The length between timestamps (%d) and values (%d) must be equal.' % (
                len(timestamps), len(values)
            )
        if len(timestamps) != len(set(timestamps)):
            return False, 'The sequence prohibits duplicate timestamp.'
        if not all(x < y for x, y in zip(timestamps, timestamps[1:])):
            return False, 'Timestamps must be strictly increasing.'
        for t in timestamps:
            if type(t) is not int:
                return False, 'The type of timestamp must be integer.'

        # valid
        return True, None

    @staticmethod
    def _create_sub_sequence(parent, ts_start, ts_end):
        """This Sequence slicing method is not the same as list slicing. List in Python
        slices from start index till (end - 1), specified as list elements.
        But the sub-sequence includes the last element, i.e., ts_end.
        """
        dbmind_assert(parent is not None, 'BUG: #1 parent should not be NoneType.')
        if OptionalContainer(getattr(parent, '_timestamps')).get(0) == ts_start and \
                OptionalContainer(getattr(parent, '_timestamps')).get(-1) == ts_end:
            return parent

        sub = Sequence(name=parent.name, labels=parent.labels, step=parent.step)
        if ts_start > ts_end:
            return sub

        sub._parent = parent
        sub._parent_start = ts_start
        sub._parent_end = ts_end
        return sub

    def _get_entity(self):
        """Sub-sequence does not store data entity. Hence, the method backtracks to the
        start node (aka, head node, ancestor node) and return a quadruple to caller to traverse.

        Return a quadruple:
        ..

            (timestamps, values, starting timestamp, ending timestamp).

        """
        this_ts_start = OptionalContainer(self._timestamps).get(0)
        this_ts_end = OptionalContainer(self._timestamps).get(-1)
        # If current sequence has no parent node, the sequence must be at the start node.
        if not self._parent:
            return self._timestamps, self._values, this_ts_start, this_ts_end

        # Since current sequence has a parent node, we should backtrack until
        # we find the start node and then return the data entity based on
        # the data recorded in the start node.
        start_node = self
        while start_node._parent is not None:
            start_node = start_node._parent

        ts_start = OptionalValue(self._parent_start).get(this_ts_start)
        ts_end = OptionalValue(self._parent_end).get(this_ts_end)
        return start_node._timestamps, start_node._values, ts_start, ts_end

    def get(self, timestamp) -> Optional[int]:
        """Get a target by timestamp.

        :return If not found, return None."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        if None in (ts_start, ts_end):
            return

        if ts_start <= timestamp <= ts_end:
            idx = binary_search(timestamps, timestamp)
            if idx < 0:
                return
            return values[idx]

    @cached_property
    def length(self):
        timestamps, _, ts_start, ts_end = self._get_entity()
        if timestamps == _EMPTY_TUPLE:
            return 0
        # Notice: this is a TRICK for binary search:
        # ``how_many_larger_elements()`` can ensure that
        # the position of the searching element always stays
        # at the position of the last element not greater than it in the array.
        start_position = binary_search_leftmost(timestamps, ts_start)
        end_position = binary_search_rightmost(timestamps, ts_end)
        return end_position - start_position + 1

    def to_2d_array(self):
        return self.timestamps, self.values

    @cached_property
    def values(self):
        """The property will generate a copy."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        return values[binary_search_leftmost(timestamps, ts_start):
                      binary_search_rightmost(timestamps, ts_end) + 1]

    @property
    def timestamps(self):
        """The property will generate a copy."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        return timestamps[binary_search_leftmost(timestamps, ts_start):
                          binary_search_rightmost(timestamps, ts_end) + 1]

    @property
    def step(self):
        if self._step is None:
            return measure_sequence_interval(self)
        return self._step

    @step.setter
    def step(self, value):
        self._step = value

    @cached_property
    def labels(self):
        return uniform_labels(self._labels)

    def copy(self):
        return Sequence(
            self.timestamps, self.values, self.name, self.step, self.labels
        )

    def __getitem__(self, item):
        """If parameter ``item`` is a two-tuple, create a sub-sequence and return it.
        If ``item`` is an integer, which represents an index (timestamp) of target, search and return
        the target.

        :exception raise ValueError while item does not belong to any valid types.
        """
        if isinstance(item, int):
            return self.get(item)
        elif isinstance(item, slice):
            raise NotImplementedError
        elif isinstance(item, tuple) and len(item) == 2:
            # To distinguish with slicing, override the tuple form.
            start = OptionalContainer(item).get(0, default=OptionalContainer(self._timestamps).get(0))
            end = OptionalContainer(item).get(1, default=OptionalContainer(self._timestamps).get(-1))
            return Sequence._create_sub_sequence(self, start, end)
        else:
            raise ValueError('Not supported %s type.' % type(item))

    def __len__(self):
        return self.length

    def __repr__(self):
        return 'Sequence[%s](%d)%s' % (self.name, self.length, self._labels)

    def __iter__(self):
        """Return a pairwise point (timestamp, value)."""
        return zip(*self.to_2d_array())

    def __add__(self, other):
        if not isinstance(other, Sequence):
            raise TypeError('The data type must be Sequence.')
        if len(self) == 0:
            return other
        if len(other) == 0:
            return self

        if other.name != self.name or other.labels != self.labels or self.step != other.step:
            specific = []
            if other.name != self.name:
                specific.append('Name: %s vs %s.' % (self.name, other.name))
            if other.labels != self.labels:
                specific.append('Labels: %s vs %s.' % (self.labels, other.labels))
            if other.step != self.step:
                specific.append('Step: %s vs %s.' % (self.step, other.step))
            raise TypeError('Cannot merge different type of sequences: ' + ' '.join(specific))

        # 使用新的智能合并策略
        return self._smart_merge(other)

    def _smart_merge(self, other):
        """智能合并两个序列，支持不对齐的序列合并"""
        sequences = [self, other]
        sequences.sort(key=lambda s: s.timestamps[0])

        first_seq, second_seq = sequences[0], sequences[1]
        first_start, first_end = first_seq.timestamps[0], first_seq.timestamps[-1]
        second_start, second_end = second_seq.timestamps[0], second_seq.timestamps[-1]

        step = self.step or 1

        # 检查是否有间隙（不连续）
        if second_start > first_end + step:
            # 如果间隙太大，抛出异常
            gap = second_start - first_end
            if gap > step * 10:  # 允许最多10个步长的间隙
                raise TypeError('Cannot merge due to large gap: %d ms (max allowed: %d ms).'
                              % (gap, step * 10))
            # 对于小间隙，创建连续序列
            return self._merge_with_gap(first_seq, second_seq, step)

        # 处理重叠情况
        if second_start <= first_end:
            return self._merge_overlapping(first_seq, second_seq)

        # 处理连续情况
        return self._merge_continuous(first_seq, second_seq)

    def _merge_with_gap(self, first_seq, second_seq, step):
        """合并有间隙的序列，在间隙中填充 None 值"""
        # 计算需要填充的时间点
        gap_start = first_seq.timestamps[-1] + step
        gap_end = second_seq.timestamps[0] - step

        gap_timestamps = []
        current_time = gap_start
        while current_time <= gap_end:
            gap_timestamps.append(current_time)
            current_time += step

        # 创建填充值（使用 None）
        gap_values = [None] * len(gap_timestamps)

        # 合并所有部分
        new_timestamps = first_seq.timestamps + gap_timestamps + second_seq.timestamps
        new_values = first_seq.values + gap_values + second_seq.values

        return Sequence(
            name=self.name,
            labels=self.labels,
            step=self.step,
            timestamps=new_timestamps,
            values=new_values
        )

    def _merge_overlapping(self, first_seq, second_seq):
        """合并重叠的序列，优先使用后面序列的数据"""
        # 找到重叠的开始位置
        overlap_start_index = how_many_lesser_elements(first_seq.timestamps, second_seq.timestamps[0])

        # 使用第一个序列的非重叠部分 + 第二个序列的全部
        new_timestamps = first_seq.timestamps[:overlap_start_index] + second_seq.timestamps
        new_values = first_seq.values[:overlap_start_index] + second_seq.values

        return Sequence(
            name=self.name,
            labels=self.labels,
            step=self.step,
            timestamps=new_timestamps,
            values=new_values
        )

    def _merge_continuous(self, first_seq, second_seq):
        """合并连续的序列"""
        new_timestamps = first_seq.timestamps + second_seq.timestamps
        new_values = first_seq.values + second_seq.values

        return Sequence(
            name=self.name,
            labels=self.labels,
            step=self.step,
            timestamps=new_timestamps,
            values=new_values
        )

    @staticmethod
    def safe_merge(seq1, seq2):
        """安全合并两个序列，失败时返回较新的序列"""
        try:
            return seq1 + seq2
        except TypeError as e:
            # 如果合并失败，返回时间范围更大的序列
            if len(seq1.timestamps) == 0:
                return seq2
            if len(seq2.timestamps) == 0:
                return seq1

            # 比较时间范围，返回覆盖范围更大的序列
            seq1_range = seq1.timestamps[-1] - seq1.timestamps[0]
            seq2_range = seq2.timestamps[-1] - seq2.timestamps[0]

            if seq2_range > seq1_range:
                return seq2
            elif seq1_range > seq2_range:
                return seq1
            else:
                # 范围相同时，返回更新的序列（结束时间更晚的）
                return seq2 if seq2.timestamps[-1] > seq1.timestamps[-1] else seq1

    def __eq__(self, other):
        if not isinstance(other, Sequence):
            return False
        if len(self) != len(other):
            return False
        return (
            self.name == other.name and
            self.labels == other.labels and
            self.timestamps == other.timestamps and
            self.values == other.values
        )

    def __hash__(self):
        return hash(
            (self.name, frozenset(self.labels.items()), self.timestamps, self.values)
        )


def measure_sequence_interval(sequence):
    """In many cases, the sequence will have missing values.
    Therefore, by constructing a histogram, the function
    selects the interval with the largest frequency as the
    step size to avoid inaccurate step size measurement
    results caused by direct averaging. """
    histogram = dict()
    timestamps = sequence.timestamps
    for i in range(0, len(timestamps) - 1):
        interval = timestamps[i + 1] - timestamps[i]
        histogram[interval] = histogram.get(interval, 0) + 1
    # Calculate the mode of the interval array.
    most_interval = most_count = 0
    for interval, count in histogram.items():
        if count > most_count:
            most_interval = interval
            most_count = count
    return int(most_interval)


EMPTY_SEQUENCE = Sequence()
