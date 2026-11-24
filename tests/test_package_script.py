# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-DBMind is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

from pathlib import Path


PACKAGE_SCRIPT = Path(__file__).resolve().parents[1] / 'package.sh'


def test_toolchain_downloads_require_verified_https():
    script = PACKAGE_SCRIPT.read_text(encoding='utf-8')
    download_commands = [
        line.strip() for line in script.splitlines()
        if line.strip().startswith('wget ')
    ]

    assert len(download_commands) == 2
    assert '--no-check-certificate' not in script
    assert all('--https-only' in command for command in download_commands)
    assert all('https://' in command for command in download_commands)
