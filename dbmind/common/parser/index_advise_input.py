# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Input validation for toolkit_index_advise / advise_index APIs.
# Kept dependency-light (sqlparse only) so security self-checks can import it.

import sqlparse

MAX_INDEX_ADVISE_SQL_LENGTH = 65536
MAX_INDEX_ADVISE_STATEMENTS = 500
ALLOWED_INDEX_ADVISE_PREFIXES = (
    'select', 'with', 'insert', 'update', 'delete', 'explain',
)


def normalize_index_advise_sqls(sqls):
    """Normalize API sqls (str or list[str]) into a single SQL text."""
    if isinstance(sqls, list):
        if not sqls:
            raise ValueError('The SQL statement is empty.')
        parts = []
        for item in sqls:
            if not isinstance(item, str):
                raise ValueError('Invalid value for parameter sqls')
            parts.append(item)
        sqls = ''.join(parts)
    if not isinstance(sqls, str) or not sqls.strip():
        raise ValueError('The SQL statement is empty.')
    return sqls


def validate_index_advise_sqls(sqls):
    """Validate user-controlled sqls before PREPARE/EXPLAIN via RpcExecutor.

    Returns the validated statement list from sqlparse.split.
    """
    sqls = normalize_index_advise_sqls(sqls)
    if len(sqls) > MAX_INDEX_ADVISE_SQL_LENGTH:
        raise ValueError('The SQL statement is too large.')
    statements = [s for s in sqlparse.split(sqls) if s and s.strip()]
    if not statements:
        raise ValueError('The SQL statement is empty.')
    if len(statements) > MAX_INDEX_ADVISE_STATEMENTS:
        raise ValueError('Too many SQL statements for index advise.')
    for stmt in statements:
        normalized = sqlparse.format(
            stmt, strip_comments=True, keyword_case='lower'
        ).strip().lower()
        if not any(normalized.startswith(prefix) for prefix in ALLOWED_INDEX_ADVISE_PREFIXES):
            raise ValueError('Unsupported SQL statement type for index advise.')
    return statements
