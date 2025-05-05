# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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

import configparser
import gzip
import hashlib
import os
import re
import shutil
import sys
import tarfile

import psycopg2
import requests
from requests.adapters import HTTPAdapter
from pexpect import ExceptionPexpect

from dbmind.common.cmd_executor import SSH
from dbmind.common.utils.checking import (
    check_ip_valid, check_port_valid
)


class ConfigParser(configparser.ConfigParser):
    def get(self, *args, delimiter='#', **kwargs):
        text = super().get(*args, **kwargs)
        if delimiter is None:
            return text
        else:
            return text.rsplit(delimiter, 1)[0].strip()


def validate_ssh_connection(pwd, username, host, port):
    try:
        client = SSH(host, username, pwd, port=int(port), timeout=3)
        client.close()
        return True
    except ExceptionPexpect:
        return False


def validate_database_connection(pwd, username, host, port, dbname):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=pwd,
            host=host,
            port=int(port),
            application_name='DBMind-deployment'
        )
        conn.close()
        return True
    except psycopg2.Error as e:
        print(e)
        return False


def parse_ip_info_from_string(s):
    if '-' in s:
        start, end = s.split('-', 1)
        start, end = start.strip(), end.strip()
        if int(start) > int(end):
            raise ValueError('end port must be less than start port.')

        return list(map(str, range(int(start), int(end) + 1)))

    elif ',' in s:
        return list(map(str.strip, s.split(',')))
    else:
        return [s.strip()]


def convert_full_width_character_to_half_width(s):
    full_to_half = {
        '、': '/', '“': '"', '’': "'", '《': '<',
        '》': '>', '【': '[', '】': ']', '。': '.',
    }
    i = 0
    L = len(s)
    transformed = ""
    while i < L:
        c = s[i]
        code = ord(c)
        if 65281 <= code <= 65374:
            code -= 65248
            transformed += chr(code)
        elif c in full_to_half:
            transformed += full_to_half[c]
        elif c == '—':
            transformed += '_'
            i += 1
        else:
            transformed += c
        i += 1

    return transformed


def download(path, url, timeout=10):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/71.0.3578.98 '
                             'Safari/537.36'}  # to mask on as a human
    s = requests.Session()  # suitable for both http and https
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))

    size = 0
    chunk_size = 1024 * 1024  # related to the downloading speed
    filename = url.rsplit('/', 1)[-1]
    filepath = os.path.join(path, filename)

    try:  # make sure that stream == True
        with s.get(url, stream=True, headers=headers, timeout=timeout, verify=False) as response:
            content_size = int(response.headers['content-length'])

            if response.status_code == 200:  # 200 means success
                print('Downloading {file}, [File size] : {size:.2f} MB'.format(file=filename,
                                                                               size=content_size / chunk_size))
                with open(filepath, 'wb') as downloaded_file:
                    for data in response.iter_content(chunk_size=chunk_size):
                        downloaded_file.write(data)
                        size += len(data)
                        rate = size / content_size
                        num = int(50 * rate)  # progress bar
                        print('\r[Downloading] :|{}| {:.2f}%'.format('█' * num + ' ' * (50 - num), float(rate * 100)),
                              end='')

                print('\n{file} downloading succeeded.'.format(file=filename))
                return True

            else:
                print("\n{file} downloading's response is abnormal.".format(file=filename))
                sys.exit(0)

    except Exception as e:
        print('\n')
        print(e)
        print('{file} The current network is abnormal, exiting...'.format(file=filename))
        sys.exit(1)


def unzip(path, filename, extract_path):
    filepath = os.path.join(path, filename)
    suffix = ".gz"
    tar_file = filename[:-len(suffix)] if filename.endswith(suffix) else filename
    tar_path = os.path.join(path, tar_file)

    print('Extracting {}.'.format(filename))
    try:
        with gzip.GzipFile(filepath) as gz:  # un-gzip
            with open(tar_path, 'wb+') as f:
                f.write(gz.read())

        with tarfile.open(tar_path) as tar:  # un-tar
            names = tar.getnames()

            if not os.path.exists(extract_path):
                print("{} didn't exist, automaticaly created.".format(extract_path))

            for name in names:
                tar.extract(name, extract_path)
                extract_target = os.path.join(extract_path, name)
                if os.path.isdir(extract_target):
                    os.chmod(extract_target, 0o700)
                else:
                    os.chmod(extract_target, 0o600)

        os.remove(tar_path)
        return True

    except Exception as e:
        if os.path.exists(tar_path):
            os.remove(tar_path)

        if os.path.exists(extract_path):
            shutil.rmtree(extract_path, ignore_errors=True)

        print(e)
        print("{}'s extraction failed.".format(filename))
        sys.exit(1)


def checksum_sha256(path, filename, sha256_checksum):
    if filename not in sha256_checksum:
        print("This file's sha256 checksum is not in the dictionary. Checksum skipped.")
        return False

    if not os.path.exists(os.path.join(path, filename)):
        print("The '{}' is not found. Checksum skipped".format(filename))
        return False

    filepath = os.path.join(path, filename)
    sha256_hash = hashlib.sha256()

    try:
        with open(filepath, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b''):
                sha256_hash.update(byte_block)
        if sha256_hash.hexdigest() == sha256_checksum[filename]:
            print("{}'s consistency was verified.".format(filename))
            return True

        else:
            print("{} may be broken, you may need to download it again.".format(filename))
            sys.exit(0)

    except Exception as e:
        print(e)
        print('Checksum unfinished.')
        sys.exit(1)


def url_generate(filename, host):
    left = filename.find('-') + 1 if "-" in filename else None
    right = filename.find(".linux") if ".linux" in filename else None
    name = filename[:left - 1]
    version = 'v' + filename[left:right]
    return '/'.join([host, name, 'releases/download', version, filename])


def download_file(file, download_path, host):
    url = url_generate(file, host)
    print('Downloading from {}, files will be placed at {}. '
          'The downloading may take a few minutes due to bad '
          'connection.'.format(url, download_path))
    main_file_downloaded = download(download_path, url, 60 * 30)
    return main_file_downloaded


def download_sha256(file, download_path, host, sha256_checksum):
    def find_sha256(path, filename):
        sha256_file = os.path.join(path, 'sha256sums.txt')
        with open(sha256_file) as f:
            lines = [line.split() for line in f.readlines()]
            for sha256, name in lines:
                if name == filename:
                    sha256_checksum[filename] = sha256
                    break

    url = url_generate(file, host)
    sha256_url = url.rsplit('/', 1)[0] + '/sha256sums.txt'
    sha256_file_downloaded = download(download_path, sha256_url, 60)
    find_sha256(download_path, file)
    return sha256_file_downloaded


def check_config_validity(section, option, value):
    if '_ports' in option:
        if '-' not in value:
            return False, 'You need to input a range. eg. (start-end)'

        start, end = value.split('-', 1)
        start, end = start.strip(), end.strip()
        if int(start) > int(end):
            return False, f'The start {start} must be fewer than the end {end}.'

        if not (check_port_valid(start) and check_port_valid(end)):
            return False, 'Invalid port for {}-{}: {}(1024-65535)'.format(section, option, value)

    elif option == 'ssh_port':
        try:
            value = int(value)
            if not 0 < value < 1024:
                return False, 'Invalid port for {}-{}: {}(1-1023)'.format(section, option, value)

        except (TypeError, ValueError):
            return False, '{} is not a integer.'.format(value)

    elif '_port' in option:
        if not check_port_valid(value):
            return False, 'Invalid port for {}-{}: {}(1024-65535)'.format(section, option, value)

    if option == 'host' or option == 'listen_address':
        if section == 'METADATABASE':
            for item in value.split(','):
                if not check_ip_valid(item.strip()):
                    return False, 'Invalid IP Address for {}-{}: {}'.format(section, option, value)
        elif not check_ip_valid(value):
            return False, 'Invalid IP Address for {}-{}: {}'.format(section, option, value)

        if option == 'listen_address' and value.strip() == '0.0.0.0':
            print("WARNING: The listen address '0.0.0.0' is unsafe.")

    if option == 'targets':
        for db_address in value.split(','):
            if not (
                    db_address.strip() and
                    db_address.count(':') == 1 and
                    db_address.count('/') == 1 and
                    db_address.find(':') < db_address.find('/')
            ):
                return False, f'Illegal db instance "{db_address}", e.g. ip:port/dbname'

            ip, port, dbname = re.split('[:/]', db_address)
            invalid = []
            if not check_ip_valid(ip.strip()):
                invalid.append(f'Invalid IP "{ip}"')
            if not check_port_valid(port.strip()):
                invalid.append(f'Invalid port "{port}" (1024-65535)')
            if not dbname.strip():
                invalid.append(f'Empty dbname "{dbname}"')
            if invalid:
                return False, ' and '.join(invalid) + f' for "{db_address}" in {section}-targets.'

    if option == 'enable_ssl' and not (value == 'True' or value == 'False'):
        return False, 'enable_ssl must be "True" or "False".'

    # Add more checks here.
    return True, None
