from functools import total_ordering


@total_ordering
class Index:
    def __init__(self, columns, estimated_size=None):
        if len(columns) == 0:
            raise ValueError("Index needs at least 1 column")
        self.columns = tuple(columns)
        self.estimated_size = estimated_size
        self.hypopg_name = None

    def __lt__(self, other):
        if len(self.columns) != len(other.columns):
            return len(self.columns) < len(other.columns)

        return self.columns < other.columns

    def __repr__(self):
        columns_string = ",".join(map(str, self.columns))
        return f"{columns_string}"

    def __eq__(self, other):
        if not isinstance(other, Index):
            return False

        return self.columns == other.columns

    def __hash__(self):
        return hash(self.columns)

    def _column_names(self):
        return [x.name for x in self.columns]

    def is_single_column(self):
        return True if len(self.columns) == 1 else False

    def table(self):
        return self.columns[0].table

    def index_idx(self):
        columns = "_".join(self._column_names())
        return f"{self.table()}_{columns}_idx"

    def joined_column_names(self):
        return ",".join(self._column_names())

    def appendable_by(self, other):
        if not isinstance(other, Index):
            return False

        if self.table() != other.table():
            return False

        if not other.is_single_column():
            return False

        if other.columns[0] in self.columns:
            return False

        return True

    def subsumes(self, other):
        if not isinstance(other, Index):
            return False
        return self.columns[:len(other.columns)] == other.columns

    def prefixes(self):
        index_prefixes = []
        for prefix_width in range(len(self.columns) - 1, 0, -1):
            index_prefixes.append(Index(self.columns[:prefix_width]))
        return index_prefixes


def index_merge(index_1, index_2):
    merged_columns = list(index_1.columns)
    for column in index_2.columns:
        if column not in index_1.columns:
            merged_columns.append(column)
    return Index(merged_columns)


def index_split(index_1, index_2):
    common_columns = []
    index_1_residual_columns = []
    for column in index_1.columns:
        if column in index_2.columns:
            common_columns.append(column)
        else:
            index_1_residual_columns.append(column)
    if len(common_columns) == 0:
        return None
    result = {Index(common_columns)}

    if len(index_1_residual_columns) > 0:
        result.add(Index(index_1_residual_columns))

    index_2_residual_columns = []
    for column in index_2.columns:
        if column not in index_1.columns:
            index_2_residual_columns.append(column)
    if len(index_2_residual_columns) > 0:
        result.add(Index(index_2_residual_columns))

    return result
