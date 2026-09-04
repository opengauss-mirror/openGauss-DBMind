# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Self-verification for RPC object_hook / initialize_cls deserialization hardening.
# Usage (repo root):
#   python tests/verify_deserialization_rce_object_hook.py
#
# Exit code 0 means all checks passed.

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbmind.common.rpc.base import (
    initialize_cls,
    RPCJSONAble,
    RPCRequest,
    RPCResponse,
    _ALLOWED_DESERIALIZE_MODULES,
)
from dbmind.common.rpc.errors import SerializationFormatError
from dbmind.common.types import Sequence

RESULTS = []


def record(case_id, title, expected, actual, passed, detail=None):
    RESULTS.append({
        'case_id': case_id, 'title': title, 'expected': expected,
        'actual': actual, 'passed': passed, 'detail': detail,
    })
    print('[%s] %s' % ('PASS' if passed else 'FAIL', title))
    print('  expected: %r' % (expected,))
    print('  actual:   %r' % (actual,))
    if detail:
        print('  detail:   %s' % detail)


def expect_reject(case_id, title, module, name, data=None):
    if data is None:
        data = {}
    try:
        initialize_cls(module, name, data)
        record(case_id, title, 'SerializationFormatError', 'accepted', False)
    except SerializationFormatError as e:
        record(case_id, title, 'SerializationFormatError', 'SerializationFormatError: %s' % e, True)


def main():
    print('=== deserialization_rce-object_hook self-verification ===')
    print()

    # A. Attack / unconstrained types rejected
    expect_reject('A1', 'reject collections.OrderedDict', 'collections', 'OrderedDict', {'a': 1})
    expect_reject('A2', 'reject datetime.date', 'datetime', 'date', [2020, 1, 2])
    expect_reject('A3', 'reject os.system', 'os', 'system', {})
    expect_reject('A4', 'reject subprocess.Popen', 'subprocess', 'Popen', {})
    expect_reject('A5', 'reject path-like module', '../x', 'Y', {})
    expect_reject('A6', 'reject non-identifier name', 'dbmind.common.rpc.base', 'RPCRequest;x', {})

    try:
        RPCJSONAble.deserialize(json.dumps({
            '_jsonable': True,
            '_dtype': ['collections', 'Counter'],
            'a': 1,
        }))
        record('A7', 'deserialize rejects Counter via object_hook', 'SerializationFormatError', 'accepted', False)
    except SerializationFormatError as e:
        record('A7', 'deserialize rejects Counter via object_hook', 'SerializationFormatError', str(e), True)
    except Exception as e:
        # json.loads may wrap; accept SerializationFormatError in cause chain
        record('A7', 'deserialize rejects Counter via object_hook', 'SerializationFormatError',
               '%s:%s' % (type(e).__name__, e), 'SerializationFormatError' in type(e).__name__ or 'not allowed' in str(e))

    # B. Legitimate RPCJSONAble still works
    seq = Sequence(range(1, 10), range(1, 10), 'test_metric')
    seq_new = RPCJSONAble.deserialize(RPCJSONAble.serialize(seq))
    record('B1', 'Sequence round-trip', True, seq == seq_new, seq == seq_new)

    dictionary = {'int': 1, 'float': 2.21, 'class': {'seq': seq},
                  'bool': True, 'list': [1, 2, '3']}
    d_new = RPCJSONAble.deserialize(RPCJSONAble.serialize(dictionary))
    record('B2', 'dict with nested Sequence round-trip', True, d_new == dictionary, d_new == dictionary)

    request = RPCRequest('username', 'password', 'sum', fargs=[1, 2])
    response = RPCResponse(request, True, 3)
    req2 = RPCRequest.from_json(request.json())
    record('B3', 'RPCRequest.from_json', True,
           req2.username == 'username' and req2.funcname == 'sum' and list(req2.args) == [1, 2],
           req2.username == 'username' and req2.funcname == 'sum' and list(req2.args) == [1, 2])
    res2 = RPCResponse.from_json(response.json())
    record('B4', 'RPCResponse.from_json result', 3, res2.result, res2.result == 3)

    # Nested malicious type inside request args must fail before auth would run
    try:
        evil_arg = json.dumps({'_jsonable': True, '_dtype': ['collections', 'OrderedDict'], 'x': 1})
        RPCRequest.from_json({
            '_jsonable': True,
            '_dtype': ['dbmind.common.rpc.base', 'RPCRequest'],
            'username': 'u', 'pwd': 'p', 'funcname': 'f',
            'args': [evil_arg], 'kwargs': {},
        })
        record('B5', 'RPCRequest args reject non-allowlisted type', 'SerializationFormatError', 'accepted', False)
    except SerializationFormatError as e:
        record('B5', 'RPCRequest args reject non-allowlisted type', 'SerializationFormatError', str(e), True)
    except Exception as e:
        record('B5', 'RPCRequest args reject non-allowlisted type', 'SerializationFormatError',
               '%s:%s' % (type(e).__name__, e), 'not allowed' in str(e) or 'SerializationFormatError' in type(e).__name__)

    # C. Allowlist contract
    record(
        'C1', 'allowlist modules',
        {'dbmind.common.rpc.base', 'dbmind.common.types.sequence'},
        set(_ALLOWED_DESERIALIZE_MODULES),
        set(_ALLOWED_DESERIALIZE_MODULES) == {
            'dbmind.common.rpc.base', 'dbmind.common.types.sequence',
        },
    )
    src = (ROOT / 'dbmind' / 'common' / 'rpc' / 'base.py').read_text(encoding='utf-8')
    record(
        'C2', 'no arbitrary cls(**data) for non-RPCJSONAble',
        True,
        'cls(**data)' not in src and 'issubclass(cls, RPCJSONAble)' in src,
        'cls(**data)' not in src and 'issubclass(cls, RPCJSONAble)' in src,
    )

    passed = sum(1 for r in RESULTS if r['passed'])
    total = len(RESULTS)
    verdict = {
        'passed': passed,
        'total': total,
        'vulnerability_fixed': passed == total,
        'summary': (
            'initialize_cls now allowlists modules and requires RPCJSONAble subclasses; '
            'arbitrary constructors (collections/datetime/os) are rejected.'
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
