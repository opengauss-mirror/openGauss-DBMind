# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
import os
import glob
import ast
import re


def get_dbmind_source_path():
    return os.path.realpath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '../dbmind')
    )


def scan_files(glob_path):
    for filename in glob.iglob(pathname=glob_path,
                               recursive=True):
        if os.path.isdir(filename):
            continue
        with open(filename, 'r', errors='ignore') as f:
            for line in f.readlines():
                yield line, filename


def is_python_code(snippet):
    suspect = snippet.strip()
    try:
        m = ast.parse(suspect)
        check_results = []
        for expr in m.body:
            if isinstance(expr, ast.Global):
                check_results.append(False)
            elif isinstance(expr, ast.BinOp):
                check_results.append(False)
            elif isinstance(expr, ast.AnnAssign):
                check_results.append(False)
            elif isinstance(expr, ast.Return):
                check_results.append(False)
            elif isinstance(expr, ast.Expr):
                if isinstance(expr.value, ast.Name):
                    check_results.append(False)
                elif isinstance(expr.value, ast.Constant):
                    check_results.append(False)
                elif isinstance(expr.value, ast.Compare):
                    check_results.append(False)
                elif isinstance(expr.value, ast.BinOp):
                    check_results.append(False)
                elif isinstance(expr.value, ast.UnaryOp):
                    check_results.append(False)
                elif isinstance(expr.value, ast.Tuple):
                    check_results.append(False)
                else:
                    check_results.append(True)
            else:
                check_results.append(True)
        return any(check_results)
    except SyntaxError:
        pass
    return False


def test_scan_commented_code():
    """Scan commented code using #"""
    snippet = []
    gathering = False
    last_filename = None
    for text, filename in scan_files(f'{get_dbmind_source_path()}/**/*.py'):
        if last_filename != filename and len(snippet) > 0:
            if is_python_code('\n'.join(snippet)):
                raise AssertionError(
                    f"found commented code: '{snippet}' at {filename}.")
            snippet.clear()

        if text.strip().startswith('#'):
            gathering = True
        else:
            if gathering is True:
                if is_python_code('\n'.join(snippet)):
                    raise AssertionError(
                        f"found commented code: '{snippet}' at {filename}.")
            gathering = False
            snippet.clear()

        if gathering is True:
            # We cannot use strip because we will lost indent.
            snippet.append(text.replace('#', '', 1))
        last_filename = filename


def test_scan_troublemaker_text():
    """Scan key words in black list."""
    black_list = ['Gauss_234', 'Huawei@123', r'(\s)assert[\s|\(]']
    for text, filename in scan_files(f'{get_dbmind_source_path()}/**/*.py'):
        for troublemaker in black_list:
            m = re.search(troublemaker, text)
            if m:
                raise AssertionError(
                    f"found troublemaker '{m[0]}' using '{troublemaker}' at {filename}!"
                )


def test_scan_gbk_encoding():
    """Scan non-unicode encoding, e.g., GBK."""
    filenames = []
    for filename in glob.iglob(pathname=f'{get_dbmind_source_path()}/**/*.conf',
                               recursive=True):
        filenames.append(filename)
    for filename in glob.iglob(pathname=f'{get_dbmind_source_path()}/**/*.py',
                               recursive=True):
        filenames.append(filename)

    for filename in filenames:
        if os.path.isdir(filename):
            continue
        with open(filename, 'r', encoding='utf8', errors='strict') as f:
            try:
                for _ in f.readlines():
                    pass
            except UnicodeDecodeError as e:
                raise AssertionError(
                    f"filename: {filename}: {e.reason}"
                )
