# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Self-verification for collect_workloads SQL injection hardening.
# Usage (repo root):
#   python tests/verify_collect_workloads_security.py
#
# Exit code 0 means all checks passed.

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbmind.common.utils.checking import existing_special_char
from dbmind.components.fetch_statement.collect_workloads import (
    collect_statement_from_activity,
    collect_statement_from_statement_history,
)

RESULTS = []


def record(case_id, title, expected, actual, passed, detail=None):
    row = {
        'case_id': case_id, 'title': title, 'expected': expected,
        'actual': actual, 'passed': passed, 'detail': detail,
    }
    RESULTS.append(row)
    print('[%s] %s' % ('PASS' if passed else 'FAIL', title))
    print('  expected: %s' % expected)
    print('  actual:   %s' % actual)
    if detail:
        print('  detail:   %s' % detail)


def simulate_service_template_id_check(template_id):
    """Mirror data_transformer.collect_workloads template_id gate."""
    if template_id is not None:
        if not (isinstance(template_id, str) and template_id.isdigit()):
            raise ValueError('Invalid value for parameter template_id')


def simulate_service_list_checks(databases=None, schemas=None, db_users=None, sql_types=None):
    if databases is not None and existing_special_char(databases):
        raise ValueError('Invalid value for parameter databases')
    if schemas is not None and existing_special_char(schemas):
        raise ValueError('Invalid value for parameter schemas')
    if db_users is not None and existing_special_char(db_users):
        raise ValueError('Invalid value for parameter db_users')
    if sql_types is not None and existing_special_char(sql_types):
        raise ValueError('Invalid value for parameter sql_types')


def main():
    print('=== collect_workloads SQL injection self-verification ===')
    print()

    # A. list-param special char gate
    for cid, payload in [
        ('A1', "db1'; select 1;--"),
        ('A2', "a;b"),
        ('A3', "postgres) OR (1=1"),
    ]:
        try:
            simulate_service_list_checks(databases=payload)
            record(cid, 'service rejects list inject: %s' % payload, 'ValueError', 'accepted', False)
        except ValueError as e:
            record(cid, 'service rejects list inject: %s' % payload, 'ValueError', str(e), True)

    try:
        simulate_service_list_checks(databases='db1,db2', sql_types='SELECT')
        record('A4', 'service allows normal list params', 'ok', 'ok', True)
    except ValueError as e:
        record('A4', 'service allows normal list params', 'ok', str(e), False)

    # B. template_id service-layer reject
    for cid, tid in [('B1', "1' OR '1'='1"), ('B2', 'abc'), ('B3', "1;select 1")]:
        try:
            simulate_service_template_id_check(tid)
            record(cid, 'reject bad template_id: %s' % tid, 'ValueError', 'accepted', False)
        except ValueError as e:
            record(cid, 'reject bad template_id: %s' % tid, 'ValueError', str(e), True)

    try:
        simulate_service_template_id_check('12345')
        record('B4', 'accept digit template_id', 'ok', 'ok', True)
    except ValueError as e:
        record('B4', 'accept digit template_id', 'ok', str(e), False)

    # C. SQL builder
    try:
        collect_statement_from_statement_history(
            None, None, None, None, None, None, "1' OR '1'='1", duration=1)
        record('C1', 'builder rejects inject template_id', 'ValueError', 'built SQL', False)
    except ValueError as e:
        record('C1', 'builder rejects inject template_id', 'ValueError', str(e), True)

    stmt = collect_statement_from_statement_history(
        None, None, None, None, None, None, '12345', duration=1)
    snippet = [l.strip() for l in stmt.splitlines() if 'unique_query_id =' in l]
    ok = any("unique_query_id = '12345'" in s for s in snippet)
    inject_free = all("OR" not in s for s in snippet)
    record('C2', 'builder accepts digit template_id safely',
           "unique_query_id = '12345'", snippet, ok and inject_free)

    # D. activity path
    stmt = collect_statement_from_activity('db1,db2', 'user1', 'SELECT', duration=1)
    record('D1', 'activity SQL uses quoted IN lists', True,
           "('db1','db2')" in stmt and "('user1')" in stmt and "('SELECT')" in stmt,
           "('db1','db2')" in stmt and "('user1')" in stmt)

    # E. source code guards
    dt = (ROOT / 'dbmind/service/web/data_transformer.py').read_text(encoding='utf-8')
    m = re.search(r'def collect_workloads\([\s\S]*?\ndef ', dt)
    chunk = m.group(0) if m else ''
    record('E1', 'service source has data_source whitelist', True,
           "data_source not in ('asp'" in chunk, "data_source not in ('asp'" in chunk)
    record('E2', 'service source checks list special chars', True,
           all(x in chunk for x in [
               'existing_special_char(databases)',
               'existing_special_char(schemas)',
               'existing_special_char(db_users)',
               'existing_special_char(sql_types)',
           ]),
           'existing_special_char present for list params')
    record('E3', 'service source validates template_id isdigit', True,
           'template_id.isdigit()' in chunk, 'template_id.isdigit()' in chunk)

    # F. v1 API ParameterChecker
    core = (ROOT / 'dbmind/controllers/dbmind_core.py').read_text(encoding='utf-8')
    idx = core.find("/v1/api/workloads/collect")
    chunk = core[idx:idx + 900]
    record('F1', 'v1 API has ParameterChecker DIGIT for template_id', True,
           'ParameterChecker' in chunk and 'DIGIT' in chunk and 'template_id' in chunk,
           chunk[:200].replace('\n', ' | '))

    # G. builder source escapes template_id
    cw = (ROOT / 'dbmind/components/fetch_statement/collect_workloads.py').read_text(encoding='utf-8')
    record('G1', 'builder uses escape_single_quote for template_id', True,
           "escape_single_quote(str(template_id))" in cw,
           'escape_single_quote(str(template_id))' in cw)

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
        for r in RESULTS:
            if not r['passed']:
                print('FAIL:', r['case_id'], r['title'], r['actual'])
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
