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

import gzip
import logging
import re
from datetime import datetime


def load_logs(log_file, log_pattern, headers, line_skip=0, encoding='UTF-8'):
    """
    Function to transform log file to dataframe
    """
    log_messages = []
    linecount = 0
    if log_file.endswith('.gz'):
        text_open = gzip.open
        read_mode = 'rt'
    else:
        text_open = open
        read_mode = 'r'

    with text_open(log_file, read_mode, encoding=encoding) as fin:
        for line in fin.readlines():
            if linecount < line_skip:
                linecount += 1
                continue
            else:
                try:
                    message = line_process(line, log_pattern, headers)
                    if message is not None:
                        message['log_line'] = linecount
                        log_messages.append(message)
                        linecount += 1
                except Exception as e:
                    logging.error('%s raised while line processing %s.', str(e), line)

    return log_messages


def line_process(line, log_pattern, headers):
    match = log_pattern.search(line.strip())
    if match is None:
        return None

    message = dict()
    for header in headers:
        message[header] = match.group(header)

    datetime_i = ' '.join([message['date'], message['time']])
    datetime_i = datetime_i.strip(".")
    try:
        message['timestamp'] = datetime.timestamp(
            datetime.strptime(datetime_i[:19], "%Y-%m-%d %H:%M:%S")
        ) * 1000
    except ValueError as e:
        logging.warning("[LOG_MINER][LOG_PROCESS] Wrong log line datetime format: %s", str(e))
        return None

    return message


def compile_logformat_pattern(logformat):
    """
    Function to generate regular expression to split log messages
    """
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    log_pattern = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            log_pattern += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            headers.append(header)
            if header in ["content", "app_name"]:
                log_pattern += '(?P<%s>.*?)' % header
            else:
                log_pattern += '(?P<%s>[\S]*?)' % header

    log_pattern = re.compile('^' + log_pattern + '$')
    return headers, log_pattern


def find_template(templates, db_version):
    """ find the template matches the DB version."""
    for version in templates:
        if db_version.startswith(version):
            return templates[version]

    return templates["else"]
