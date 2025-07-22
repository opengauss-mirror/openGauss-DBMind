# MIT License
#
# Copyright (c) 2020 Hasso-Plattner-Institut
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
from .index import Index


class Workload:
    def __init__(self, queries):
        self.queries = queries

    def indexable_columns(self):
        indexable_columns = set()
        for query in self.queries:
            indexable_columns |= set(query.columns)

        return sorted(list(indexable_columns))

    def potential_indexes(self):
        return sorted([Index([c]) for c in self.indexable_columns()])


class Column:
    def __init__(self, name):
        self.name = name.lower()
        self.table = None

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return f"{self.table}.{self.name}"

    def __eq__(self, other):
        if not isinstance(other, Column):
            return False

        return self.table.name == other.table.name and self.name == other.name

    def __hash__(self):
        return hash((self.name, self.table.name))


class Table:
    def __init__(self, name):
        self.name = name.lower()
        self.columns = []

    def __lt__(self, other):
        return self.name < other.name

    def add_column(self, column):
        column.table = self
        self.columns.append(column)

    def add_columns(self, columns):
        for column in columns:
            self.add_column(column)

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False

        return self.name == other.name and tuple(self.columns) == tuple(other.columns)

    def __hash__(self):
        return hash((self.name, tuple(self.columns)))


class Query:
    def __init__(self, query_id, query_text, columns=None):
        self.nr = query_id
        self.text = query_text

        if columns is None:
            self.columns = []
        else:
            self.columns = columns

    def __repr__(self):
        return f"Q{self.nr}"
