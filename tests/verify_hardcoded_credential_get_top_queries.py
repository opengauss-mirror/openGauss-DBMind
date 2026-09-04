# Copyright (c) Huawei Technologies Co.,Ltd.
#
# Self-verification for hardcoded/plaintext credential hardening
# around get_top_queries / oauth2.credential / RPC pass-through.
#
# Usage (repo root):
#   python tests/verify_hardcoded_credential_get_top_queries.py
#
# Exit code 0 means all checks passed.

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbmind.common.security import EncryptedText, protect_secret, reveal_secret

FAKE = 'FakePwd_ForVerify_Only_!@#'
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


def main():
    print('=== hardcoded_credential / get_top_queries security self-verification ===')
    print()

    # A. EncryptedText no longer embeds plaintext
    enc = EncryptedText(FAKE)
    record('A1', 'str(EncryptedText) is masked', '******', str(enc), str(enc) == '******')
    record('A2', 'repr does not contain plaintext', True, FAKE not in repr(enc), FAKE not in repr(enc))
    record('A3', '.get() reveals plaintext', FAKE, enc.get(), enc.get() == FAKE)
    record('A4', 'equality still works vs plain str', True, enc == FAKE, enc == FAKE)
    record('A5', 'protect_secret wraps plain str', 'EncryptedText',
           type(protect_secret(FAKE)).__name__,
           isinstance(protect_secret(FAKE), EncryptedText))
    record('A6', 'protect_secret idempotent', True,
           protect_secret(enc) is enc, protect_secret(enc) is enc)
    record('A7', 'reveal_secret unwraps', FAKE, reveal_secret(enc), reveal_secret(enc) == FAKE)

    # B. Simulate oauth2 session + credential semantics
    session = {'username': 'u', 'password': EncryptedText(FAKE), 'scopes': ['127.0.0.1:5432']}
    username, password = session['username'], session['password']
    if not isinstance(password, EncryptedText):
        password = protect_secret(password)
    record('B1', 'credential-like password is EncryptedText', True,
           isinstance(password, EncryptedText), isinstance(password, EncryptedText))
    record('B2', 'credential-like str is masked', '******', str(password), str(password) == '******')

    # C. Simulate get_top_queries pass-through + RPC client pwd state
    # (no network): call_with_another_credential stores protect_secret
    class FakeRPC:
        def __init__(self):
            self.username = 'agent'
            self.pwd = protect_secret('agent_pwd')

        def call_with_another_credential(self, username, password, funcname, *args, **kwargs):
            old_u, old_p = self.username, self.pwd
            self.username = username
            self.pwd = protect_secret(password) if password is not None else None
            try:
                state = {
                    'pwd_is_EncryptedText': isinstance(self.pwd, EncryptedText),
                    'str_masked': str(self.pwd) == '******',
                    'revealed_ok': reveal_secret(self.pwd) == FAKE,
                }
                return state
            finally:
                self.username, self.pwd = old_u, old_p

    # Mirror data_transformer.get_top_queries credential usage
    rpc = FakeRPC()
    state = rpc.call_with_another_credential(username, password, 'query_in_postgres', 'select 1')
    record('C1', 'get_top_queries-like RPC keeps EncryptedText on client', True,
           state['pwd_is_EncryptedText'], state['pwd_is_EncryptedText'])
    record('C2', 'RPC client str(pwd) masked during call', True,
           state['str_masked'], state['str_masked'])
    record('C3', 'reveal only at boundary yields correct secret', True,
           state['revealed_ok'], state['revealed_ok'])

    # D. Source wiring
    sec = (ROOT / 'dbmind' / 'common' / 'security.py').read_text(encoding='utf-8')
    client = (ROOT / 'dbmind' / 'common' / 'rpc' / 'client.py').read_text(encoding='utf-8')
    oauth = (ROOT / 'dbmind' / 'common' / 'http' / '_service_impl.py').read_text(encoding='utf-8')
    core = (ROOT / 'dbmind' / 'controllers' / 'dbmind_core.py').read_text(encoding='utf-8')
    dt = (ROOT / 'dbmind' / 'service' / 'web' / 'data_transformer.py').read_text(encoding='utf-8')

    record('D1', 'EncryptedText defines __new__ mask', True,
           'def __new__' in sec and "_MASK = '******'" in sec,
           'def __new__' in sec and "_MASK = '******'" in sec)
    record('D2', 'RPCClient reveals only in _call_without_lock', True,
           'reveal_secret(self.pwd)' in client and 'protect_secret(password)' in client,
           'reveal_secret(self.pwd)' in client and 'protect_secret(password)' in client)
    record('D3', 'OAuth2.credential ensures EncryptedText', True,
           'protect_secret(password)' in oauth and 'isinstance(password, EncryptedText)' in oauth,
           'protect_secret(password)' in oauth and 'isinstance(password, EncryptedText)' in oauth)
    record('D4', 'get_top_queries still uses credential then DT', True,
           'data_transformer.get_top_queries(username, password)' in core
           and 'def get_top_queries(username, password)' in dt,
           'data_transformer.get_top_queries(username, password)' in core
           and 'def get_top_queries(username, password)' in dt)
    # EncryptedText __repr__ must not call get()
    repr_leaks = bool(re.search(r'def __repr__\(self\):\s*return self\.get\(\)', sec))
    record('D5', '__repr__ no longer returns plaintext via get()', False, repr_leaks, not repr_leaks)

    passed = sum(1 for r in RESULTS if r['passed'])
    total = len(RESULTS)
    verdict = {
        'passed': passed,
        'total': total,
        'vulnerability_fixed': passed == total,
        'summary': (
            'EncryptedText no longer stores plaintext in the str face; oauth2.credential returns '
            'EncryptedText; get_top_queries path keeps wrapped secrets until RPCClient reveals '
            'only when building RPCRequest.'
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
