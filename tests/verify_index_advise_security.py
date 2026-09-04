# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Self-verification for toolkit_index_advise / advise_index SQL hardening.
# Usage (repo root):
#   python tests/verify_index_advise_security.py
#
# Exit code 0 means all checks passed (vulnerability path closed at entry).

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbmind.common.parser.index_advise_input import (
    validate_index_advise_sqls,
    normalize_index_advise_sqls,
    ALLOWED_INDEX_ADVISE_PREFIXES,
    MAX_INDEX_ADVISE_SQL_LENGTH,
)
from dbmind.components.index_advisor.sql_generator import get_prepare_sqls

RESULTS = []


def record(case_id, title, expected, actual, passed, detail=None):
    row = {
        'case_id': case_id, 'title': title, 'expected': expected,
        'actual': actual, 'passed': passed, 'detail': detail,
    }
    RESULTS.append(row)
    print('[%s] %s' % ('PASS' if passed else 'FAIL', title))
    print('  expected: %r' % (expected,))
    print('  actual:   %r' % (actual,))
    if detail:
        print('  detail:   %s' % detail)


def expect_reject(case_id, title, sql):
    try:
        validate_index_advise_sqls(sql)
        record(case_id, title, 'ValueError', 'accepted (NO ERROR)', False)
    except ValueError as e:
        record(case_id, title, 'ValueError', 'ValueError: %s' % e, True)


def expect_accept(case_id, title, sql):
    try:
        stmts = validate_index_advise_sqls(sql)
        record(case_id, title, 'accept', 'accepted (%d stmt)' % len(stmts), True)
    except ValueError as e:
        record(case_id, title, 'accept', 'ValueError: %s' % e, False)


def main():
    print('=== toolkit_index_advise security self-verification ===')
    print('cwd/repo:', ROOT)
    print()

    # A. Attack / unsafe payloads rejected at entry
    expect_reject('A1', 'empty sql', '')
    expect_reject('A2', 'DROP DDL', 'drop table secrets;')
    expect_reject('A3', 'TRUNCATE DDL', 'truncate table secrets;')
    expect_reject('A4', 'ALTER DDL', 'alter table t add column x int;')
    expect_reject('A5', 'CREATE DDL', 'create table evil(id int);')
    expect_reject('A6', 'GRANT', 'grant all on t to public;')
    expect_reject('A7', 'SET session', "set role dba;")
    expect_reject('A8', 'COPY', 'copy t from stdin;')
    expect_reject('A9', 'mixed SELECT+DROP', 'select 1; drop table secrets;')
    expect_reject('A10', 'oversized sql', 'a' * (MAX_INDEX_ADVISE_SQL_LENGTH + 1))

    # B. Benign index-advise workloads accepted
    expect_accept('B1', 'SELECT', 'select * from t1 where id=1;')
    expect_accept('B2', 'WITH', 'with cte as (select 1 as a) select * from cte;')
    expect_accept('B3', 'multi SELECT', 'select * from t1; select * from t2 where a=1;')
    expect_accept('B4', 'DELETE (ratio analysis)', 'delete from t1 where id=1;')
    expect_accept('B5', 'UPDATE', 'update t1 set a=1 where id=2;')
    expect_accept('B6', 'INSERT', 'insert into t1 values (1);')
    expect_accept('B7', 'list input', ['select * from t1;'])

    # C. normalize + allowlist contract
    record(
        'C1', 'allowlist covers DML used by is_dml + explain',
        ('select', 'with', 'insert', 'update', 'delete', 'explain'),
        ALLOWED_INDEX_ADVISE_PREFIXES,
        ALLOWED_INDEX_ADVISE_PREFIXES == (
            'select', 'with', 'insert', 'update', 'delete', 'explain',
        ),
    )
    joined = normalize_index_advise_sqls(['select 1;', 'select 2;'])
    record('C2', 'list normalize joins', 'select 1;select 2;', joined, joined == 'select 1;select 2;')

    # D. Source wiring: entry calls validate before rpc path
    dt = (ROOT / 'dbmind' / 'service' / 'web' / 'data_transformer.py').read_text(encoding='utf-8')
    idx = dt.find('def toolkit_index_advise(')
    chunk = dt[idx:idx + 2200]
    record(
        'D1', 'toolkit_index_advise calls validate_index_advise_sqls',
        True,
        'validate_index_advise_sqls' in chunk and 'rpc_index_advise' in chunk,
        'validate_index_advise_sqls' in chunk and 'rpc_index_advise' in chunk,
    )
    record(
        'D2', 'validate runs before agent_proxy / RpcExecutor',
        True,
        chunk.find('validate_index_advise_sqls') < chunk.find('RpcExecutor'),
        chunk.find('validate_index_advise_sqls') < chunk.find('RpcExecutor'),
    )

    core = (ROOT / 'dbmind' / 'controllers' / 'dbmind_core.py').read_text(encoding='utf-8')
    v1_start = core.find("/v1/api/toolkit/advise/index")
    v1_end = core.find("@request_mapping('/v1/api/values'", v1_start)
    v1 = core[v1_start:v1_end]
    record(
        'D3', 'v1 advise/index has ParameterChecker',
        True,
        'ParameterChecker' in v1,
        'ParameterChecker' in v1,
    )
    bad_call = 'toolkit_index_advise(username, password, current, pagesize, instance, database'
    good_call = 'toolkit_index_advise(username, password, instance, database'
    record(
        'D4', 'v1 call no longer shifts args with current/pagesize',
        True,
        {'good_call': good_call in v1, 'bad_call': bad_call in v1},
        good_call in v1 and bad_call not in v1,
    )

    old = (ROOT / 'dbmind' / 'service' / 'web' / 'old_data_transformer.py').read_text(encoding='utf-8')
    oidx = old.find('def toolkit_index_advise(')
    ochunk = old[oidx:oidx + 800]
    record(
        'D5', 'old_data_transformer also validates',
        True,
        'validate_index_advise_sqls' in ochunk,
        'validate_index_advise_sqls' in ochunk,
    )

    # E. Rejected DDL never reaches prepare embedding via validate gate
    try:
        validate_index_advise_sqls('drop table secrets')
        gated = False
    except ValueError:
        gated = True
    prep_if_forced = get_prepare_sqls('drop table secrets', verbose=False, is_m_compat=False)
    record(
        'E1', 'DDL blocked at validate (prepare helper still raw if forced)',
        'gated=True',
        {'gated': gated, 'raw_prepare_if_forced': prep_if_forced[2]},
        gated is True,
        detail='Entry gate is the security control; get_prepare_sqls remains advisor-internal.',
    )

    passed = sum(1 for r in RESULTS if r['passed'])
    total = len(RESULTS)
    verdict = {
        'passed': passed,
        'total': total,
        'vulnerability_fixed': passed == total,
        'summary': (
            'Entry validate_index_advise_sqls rejects DDL/admin SQL; only DML/EXPLAIN '
            'reach RpcExecutor/rpc_index_advise. ParameterChecker on v1 advise/index; '
            'call signature aligned.'
        ),
    }
    print()
    print('=== SUMMARY ===')
    print(json.dumps(verdict, ensure_ascii=False, indent=2))

    if passed != total:
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
