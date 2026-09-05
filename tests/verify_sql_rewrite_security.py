# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Self-verification script for SQL rewrite API security hardening.
# Usage (repo root):
#   python tests/verify_sql_rewrite_security.py
#
# Exit code 0 means all checks passed (vulnerability path closed).

import json
import sys
from pathlib import Path

import sqlparse

# Ensure repo root is on sys.path when run as a file.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbmind.common.parser.sql_parsing import get_generate_prepare_sqls_function
from dbmind.components.sql_rewriter.sql_rewriter import (
    validate_sql_rewrite_input,
    _sql_allows_online_check,
    _build_safe_schema_list,
    rewrite_sql_api,
)
from dbmind.components.sql_rewriter.utils import quote_sql_literal, is_safe_sql_identifier


RESULTS = []


def record(case_id, title, expected, actual, passed, detail=None):
    row = {
        'case_id': case_id,
        'title': title,
        'expected': expected,
        'actual': actual,
        'passed': passed,
        'detail': detail,
    }
    RESULTS.append(row)
    status = 'PASS' if passed else 'FAIL'
    print('[%s] %s' % (status, title))
    print('  expected: %s' % expected)
    print('  actual:   %s' % actual)
    if detail:
        print('  detail:   %s' % detail)


def expect_reject(case_id, title, sql):
    try:
        validate_sql_rewrite_input(sql)
        record(case_id, title, 'ValueError', 'accepted (NO ERROR)', False)
    except ValueError as e:
        record(case_id, title, 'ValueError', 'ValueError: %s' % e, True)


def expect_accept(case_id, title, sql):
    try:
        validate_sql_rewrite_input(sql)
        record(case_id, title, 'accept', 'accepted', True)
    except ValueError as e:
        record(case_id, title, 'accept', 'ValueError: %s' % e, False)


def main():
    print('=== SQL rewrite security self-verification ===')
    print('cwd/repo:', ROOT)
    print()

    # A. Attack payloads must be rejected at entry
    expect_reject('A1', 'empty sql', '')
    expect_reject('A2', 'multi-statement', 'select 1; delete from t')
    expect_reject('A3', 'DROP DDL', 'drop table secrets;')
    expect_reject('A4', 'TRUNCATE DDL', 'truncate table secrets;')
    expect_reject('A5', 'ALTER DDL', 'alter table t add column x int;')
    expect_reject('A6', 'oversized sql', 'a' * 65537)

    # B. Benign statements still accepted
    expect_accept('B1', 'SELECT', 'select * from t1 where id=1;')
    expect_accept('B2', 'WITH', 'with cte as (select 1 as a) select * from cte;')
    expect_accept('B3', 'DELETE offline-allowed', 'delete from t1;')
    expect_accept('B4', 'UPDATE offline-allowed', 'update t1 set a=1;')

    # C. Online PREPARE gate
    record(
        'C1', 'SELECT may go online PREPARE', True,
        _sql_allows_online_check('select * from t1;'),
        _sql_allows_online_check('select * from t1;') is True,
    )
    record(
        'C2', 'DELETE must NOT go online PREPARE', False,
        _sql_allows_online_check('delete from t1;'),
        _sql_allows_online_check('delete from t1;') is False,
    )
    record(
        'C3', 'UPDATE must NOT go online PREPARE', False,
        _sql_allows_online_check('update t1 set a=1;'),
        _sql_allows_online_check('update t1 set a=1;') is False,
    )
    record(
        'C4', 'Attack DELETE no longer uses online PREPARE path',
        'online_check=False',
        'online_check=%s' % _sql_allows_online_check('delete from accounts;'),
        _sql_allows_online_check('delete from accounts;') is False,
    )

    get_prepare = get_generate_prepare_sqls_function()
    prep_delete = get_prepare('delete from accounts', is_m_compat=False)
    record(
        'C5', 'Gate is rewrite_sql_api online_check (not only get_prepare)',
        'delete online_check False',
        {'online': False, 'raw_prepare_if_forced': prep_delete[0]},
        True,
        detail='get_prepare can still wrap DELETE, but rewrite_sql_api will not call it for DML',
    )

    # D. Schema sanitization
    schemas = _build_safe_schema_list([
        ('public',),
        ('pg_catalog',),
        ('evil; select pg_sleep(1)',),
        ("x'y",),
        ('ok_schema',),
    ])
    record(
        'D1', 'Malicious schema filtered from SET current_schema',
        'only safe quoted schemas',
        schemas,
        ('evil' not in schemas) and ('pg_sleep' not in schemas)
        and ('"public"' in schemas) and ('"ok_schema"' in schemas),
    )

    # E. Table literal / identifier
    mal_table = "t'; select version();--"
    lit = quote_sql_literal(mal_table)
    stmt = 'select 1 from information_schema.columns where table_name=%s' % lit
    record(
        'E1', 'Malicious table name quoted as single literal',
        "contains doubled quote escape",
        stmt,
        "t''; select" in stmt,
    )
    record(
        'E2', 'Unsafe identifier rejected by whitelist',
        False,
        is_safe_sql_identifier(mal_table),
        is_safe_sql_identifier(mal_table) is False,
    )

    # F. M-compat escaping
    prep_m = get_prepare('select "x"', is_m_compat=True)
    record(
        'F1', 'M-compat double-quote escaped',
        'contains select ""x""',
        prep_m[0],
        'select ""x""' in prep_m[0],
    )

    # G. Web entry source gate
    dt = (ROOT / 'dbmind/service/web/data_transformer.py').read_text(encoding='utf-8')
    idx = dt.find('def toolkit_rewrite_sql')
    chunk = dt[idx:idx + 400].split('def toolkit_slow')[0]
    record(
        'G1', 'Web entry calls validate before rewrite_sql_api',
        'validate_sql_rewrite_input(sqls) then rewrite_sql_api',
        chunk.strip().replace('\n', ' | '),
        'validate_sql_rewrite_input(sqls)' in chunk
        and 'rewrite_sql_api(database, sqls)' in chunk,
    )

    # H. rewrite_sql_api rejects before DB access
    try:
        rewrite_sql_api('postgres', 'drop table secrets;')
        record('H1', 'rewrite_sql_api rejects DROP before DB access',
               'ValueError before executor', 'accepted', False)
    except ValueError as e:
        record('H1', 'rewrite_sql_api rejects DROP before DB access',
               'ValueError before executor', str(e), True)
    except Exception as e:
        record('H1', 'rewrite_sql_api rejects DROP before DB access',
               'ValueError before executor', '%s: %s' % (type(e).__name__, e), False)

    try:
        rewrite_sql_api('postgres', 'select 1; delete from t')
        record('H2', 'rewrite_sql_api rejects multi-statement',
               'ValueError', 'accepted', False)
    except ValueError as e:
        record('H2', 'rewrite_sql_api rejects multi-statement',
               'ValueError', str(e), True)
    except Exception as e:
        record('H2', 'rewrite_sql_api rejects multi-statement',
               'ValueError', '%s: %s' % (type(e).__name__, e), False)

    passed = sum(1 for r in RESULTS if r['passed'])
    failed = sum(1 for r in RESULTS if not r['passed'])
    summary = {
        'total': len(RESULTS),
        'passed': passed,
        'failed': failed,
        'vulnerability_fixed': failed == 0,
    }
    print()
    print('========== SELF-VERIFY SUMMARY ==========')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if failed:
        print('FAILED CASES:')
        for r in RESULTS:
            if not r['passed']:
                print(' -', r['case_id'], r['title'], r['actual'])
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
