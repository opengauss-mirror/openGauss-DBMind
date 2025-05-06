# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

import argparse
import datetime
import logging
import os
import re
import socket
import subprocess
from functools import wraps, lru_cache
from urllib.parse import urlparse

from dbmind.common.utils import write_to_terminal

from .base import ignore_exc

v4_exp = r"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}"
IPV4_PATTERN = re.compile(rf"^{v4_exp}$")
v6_seg = r"[0-9A-Fa-f]{1,4}"
v6_exp = (
    rf"((({v6_seg}:){{7}}({v6_seg}|:))|"
    rf"(({v6_seg}:){{6}}(((:{v6_seg}){{1,1}})|({v4_exp})|:))|"
    rf"(({v6_seg}:){{5}}(((:{v6_seg}){{1,2}})|:({v4_exp})|:))|"
    rf"(({v6_seg}:){{4}}(((:{v6_seg}){{1,3}})|((:{v6_seg}){{0,1}}:({v4_exp}))|:))|"
    rf"(({v6_seg}:){{3}}(((:{v6_seg}){{1,4}})|((:{v6_seg}){{0,2}}:({v4_exp}))|:))|"
    rf"(({v6_seg}:){{2}}(((:{v6_seg}){{1,5}})|((:{v6_seg}){{0,3}}:({v4_exp}))|:))|"
    rf"(({v6_seg}:){{1}}(((:{v6_seg}){{1,6}})|((:{v6_seg}){{0,4}}:({v4_exp}))|:))|"
    rf"(:({v6_seg}:){{0}}(((:{v6_seg}){{1,7}})|((:{v6_seg}){{0,5}}:({v4_exp}))|:)))"
)
IPV6_PATTERN = re.compile(rf"(^\[{v6_exp}(%.+)?]$)|(^{v6_exp}(%.+)?$)")
BARE_IPV6_PATTERN = re.compile(rf"^{v6_exp}(%.+)?$")
port_exp = r"(102[4-9]|10[3-9]\d|1[1-9]\d{2}|[2-9]\d{3}|[1-5]\d{4}|6[0-4]\d{3}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])"
WITH_PORT = re.compile(rf"^{v4_exp}:{port_exp}$|^\[{v6_exp}(%.+)?]:{port_exp}$")
WITH_REGEX_PORT = re.compile(rf"(^\[{v6_exp}]|{v4_exp})\(.*?\)$")
INSTANCE_PATTERN = re.compile(rf"(^{v4_exp}(:{port_exp}|)$)|(^\[{v6_exp}(%.+)?]:{port_exp}$)|(^{v6_exp}(%.+)?$)")
LOCAL_INSTANCE_PATTERN = re.compile(rf"(^0\.0\.0\.0:{port_exp}$)|(^\[::(%.+)?]:{port_exp}$)")
NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]{2,120}$")
MAX_STRING_LENGTH = 10240
# timestamp which has 13 digits
TIMESTAMPS_PATTERN = re.compile(r"^\d{13}$")
TIMEZONE_PATTERN = re.compile(r"UTC[-+]\d{1,2}(:\d{1,2})?$")

v4_seg = r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"
v6_instance = [rf"{(v6_seg + ':') * 7}{v6_seg}",
               rf"{(v6_seg + ':') * 7}:",
               rf"{(v6_seg + ':') * 6}{(':' + v6_seg) * 1}",
               rf"{(v6_seg + ':') * 6}{v4_seg}",
               rf"{(v6_seg + ':') * 6}:",
               rf":{(':' + v6_seg) * 1}",
               rf":{(':' + v6_seg) * 2}",
               rf":{(':' + v6_seg) * 3}",
               rf":{(':' + v6_seg) * 4}",
               rf":{(':' + v6_seg) * 5}",
               rf":{(':' + v6_seg) * 6}",
               rf":{(':' + v6_seg) * 7}",
               rf":{(':' + v6_seg) * 0}:{v4_seg}",
               rf":{(':' + v6_seg) * 1}:{v4_seg}",
               rf":{(':' + v6_seg) * 2}:{v4_seg}",
               rf":{(':' + v6_seg) * 3}:{v4_seg}",
               rf":{(':' + v6_seg) * 4}:{v4_seg}",
               rf":{(':' + v6_seg) * 5}:{v4_seg}",
               rf"::"]

for idx in range(2, 7):
    for a in range(1, idx + 1):
        v6_instance.append(rf"{(v6_seg + ':') * (7 - idx)}{(':' + v6_seg) * a}")
    for b in range(idx - 1):
        v6_instance.append(rf"{(v6_seg + ':') * (7 - idx)}{(':' + v6_seg) * b}:{v4_seg}")
    v6_instance.append(rf"{(v6_seg + ':') * (7 - idx)}:")

for idx in range(len(v6_instance)):
    v6_instance.append(rf"\[{v6_instance[idx]}]:[0-9]{{4,5}}")
    v6_instance.append(rf"\[{v6_instance[idx]}]\(.*?\)")

v6_instance = "|".join(v6_instance)
SPLIT_INSTANCES_PATTERN = re.compile(rf"({v6_instance}|{v4_seg}:[0-9]{{4,5}}|{v4_seg}\(.*?\)|{v4_seg})")


def uniform_ip(ip):
    ip = ip.replace("'", '').replace('"', '').strip()
    if not ip:
        return ip

    return socket.getaddrinfo(ip, port=None)[0][4][0]


def soft_uniform_ip(ip):
    try:
        return uniform_ip(ip)
    except (socket.gaierror, OSError):
        return ip


def uniform_instance(instance):
    if not isinstance(instance, str) or not instance.strip():
        return instance

    instance = instance.replace("'", '').replace('"', '').strip()

    res = split_ip_port(instance)
    if WITH_PORT.match(instance):
        ip, port = res
        return f"{prepare_ip(ip)}:{port}"
    elif len(res) == 2:
        ip, port_pattern = res
        return f"{prepare_ip(ip)}{port_pattern}"
    else:
        return res[0]


def uniform_url(url):
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    new_host = uniform_instance(host)
    return url.replace(host, new_host)


def uniform_labels(labels):
    if 'from_instance' in labels:
        labels['from_instance'] = uniform_instance(labels['from_instance'])

    if 'instance' in labels:
        labels['instance'] = uniform_instance(labels['instance'])

    url = labels.get('url')
    if isinstance(url, str) and url.startswith(('http://', 'https://')):
        labels['url'] = uniform_url(url)

    return labels


def ipv6_full_match(ip):
    def v6_to_v4(v6_a, v6_b):
        int_a, int_b = int(v6_a, 16), int(v6_b, 16)
        return f"{int_a >> 8}.{int_a & 255}.{int_b >> 8}.{int_b & 255}"

    def v4_to_v6(ipv4):
        s1, s2, s3, s4 = ipv4.split(".")
        return hex((int(s1) << 8) + int(s2))[2:], hex((int(s3) << 8) + int(s4))[2:]

    if ip == "::":
        return ip

    zero_exp = "[0]{1,4}"
    mode = None
    simple_ip = uniform_ip(ip)
    simple = simple_ip.split(":")
    if simple_ip.startswith("::"):
        mode = "left"
        simple = simple[1:]
    elif simple_ip.endswith("::"):
        mode = "right"
        simple = simple[:-1]
    elif "::" in simple_ip:
        mode = "mid"

    if IPV4_PATTERN.match(simple[-1]):
        x, y = v4_to_v6(simple[-1])
        simple = simple[:-1] + [x, y]

    v6_n_hidden = 9 - len(simple) if mode else 0
    v4_n_hidden = max(0, v6_n_hidden - 2)

    tail = False
    full = list()
    for i, element in enumerate(simple):
        if element:
            full.append(element)
            if tail:
                v4_n_hidden = min(v6_n_hidden, v4_n_hidden + 1)
        else:
            full.extend(["0000"] * v6_n_hidden)
            tail = True

    v4_suffix = v6_to_v4(full[-2], full[-1])

    v6_segments = []
    v4_segments = []
    n = 0
    for i, element in enumerate(simple):
        length = len(element)
        if length:
            if length == 4:
                segment = element
            else:
                segment = f"[0]{{0,{4 - length}}}{element}"

            if element.isdigit():
                segment = f"({segment})"
            else:
                segment = f"(?i:{segment})"

            v6_segments.append(segment)
            if n < 6:
                v4_segments.append(segment)

            n += 1

        else:
            v6_segment = [f"({zero_exp}:){{{v6_n_hidden - 1}}}({zero_exp})"]
            v4_segment = [f"({zero_exp}:){{{v4_n_hidden - 1}}}({zero_exp})"]
            if mode in ["left", "right"]:
                v6_segment = [":"] + v6_segment
                v4_segment = [":"] + v4_segment
                if mode == "left":
                    v6_segment = v6_segment + [
                        f"({zero_exp}:){{{v6_n_hidden - 1}}}",
                    ] + [
                        f"({zero_exp}:){{{j}}}(:{zero_exp}){{0,{v6_n_hidden - 1 - j}}}"
                        for j in range(1, v6_n_hidden - 1)
                    ]
                    v4_segment = v4_segment + [
                        f"({zero_exp}:){{{v4_n_hidden - 1}}}",
                    ] + [
                        f"({zero_exp}:){{{j}}}(:{zero_exp}){{0,{v4_n_hidden - 1 - j}}}"
                        for j in range(1, v4_n_hidden - 1)
                    ]
                elif mode == "right":
                    v6_segment = v6_segment + [
                        f"(:{zero_exp}){{{v6_n_hidden - 1}}}",
                    ] + [
                        f"({zero_exp}:){{0,{v6_n_hidden - j - 1}}}(:{zero_exp}){{{j}}}"
                        for j in range(1, v6_n_hidden - 1)
                    ]
                    v4_segment = v4_segment + [
                        f"(:{zero_exp}){{{v4_n_hidden - 1}}}",
                    ] + [
                        f"({zero_exp}:){{0,{v4_n_hidden - j - 1}}}(:{zero_exp}){{{j}}}"
                        for j in range(1, v4_n_hidden - 1)
                    ]
            elif mode == "mid":
                v6_segment = [""] + v6_segment
                v4_segment = [""] + v4_segment
                v6_segment = v6_segment + [
                    f"({zero_exp}:){{{v6_n_hidden - 1}}}"
                    f"(:{zero_exp}){{1,{v6_n_hidden - 1}}}",
                ] + [
                    f"({zero_exp}:){{{j}}}(:{zero_exp}){{0,{v6_n_hidden - 1 - j}}}"
                    for j in range(1, v6_n_hidden - 1)
                ]
                v4_segment = v4_segment + [
                    f"({zero_exp}:){{{v4_n_hidden - 1}}}"
                    f"(:{zero_exp}){{1,{v4_n_hidden - 1}}}",
                ] + [
                    f"({zero_exp}:){{{j}}}(:{zero_exp}){{0,{v4_n_hidden - 1 - j}}}"
                    for j in range(1, v4_n_hidden - 1)
                ]
            else:
                continue

            v6_segment = "|".join(v6_segment)
            v4_segment = "|".join(v4_segment)

            v6_segments.append(f"({v6_segment})")
            if n < 6:
                v4_segments.append(f"({v4_segment})")

            n += v6_n_hidden

    v4_segments.append(v4_suffix)

    v6_pattern = ":".join(v6_segments)
    v4_pattern = ":".join(v4_segments)

    pattern = f"(({v6_pattern})|({v4_pattern}))"

    return pattern


def transform_instance(instance, full_match=True):
    res = split_ip_port(instance)
    if not IPV6_PATTERN.match(res[0]):
        return rf"^{instance}$"

    ip_pattern = res[0] if not full_match else ipv6_full_match(res[0])
    if len(res) != 2:
        return rf"^{ip_pattern}$"

    port = res[1]
    if WITH_PORT.match(instance):
        return rf"^\\[{ip_pattern}]:{port}$"
    else:
        return rf"^\\[{ip_pattern}]{port}$"


def check_path_valid(path):
    char_black_list = (" ", "|", ";", "&", "$", "<", ">", "`", "\\",
                       "'", "\"", "{", "}", "(", ")", "[", "]", "~",
                       "*", "?", "!", "\n")

    if path.strip() == '':
        return True

    for char in char_black_list:
        if path.find(char) >= 0:
            return False

    return True


@lru_cache(maxsize=None)
def check_ip_valid(value):
    if not isinstance(value, str) or not value.strip():
        return False

    if IPV4_PATTERN.match(value) or BARE_IPV6_PATTERN.match(value):
        return True

    return False


@lru_cache(maxsize=None)
def prepare_ip(ip):
    ip = soft_uniform_ip(ip)
    if BARE_IPV6_PATTERN.match(ip):
        return f"[{ip}]"
    else:
        return ip


@lru_cache(maxsize=None)
def split_ip_port(instance):
    if WITH_REGEX_PORT.match(instance):
        ip, port_pattern = instance.split("(", 1)
        return soft_uniform_ip(ip.strip("[]")), f"({port_pattern}"
    elif WITH_PORT.match(instance):
        ip, port = instance.rsplit(":", 1)
        return soft_uniform_ip(ip.strip("[]")), port
    else:
        return soft_uniform_ip(instance.strip("[]")),


@lru_cache(maxsize=None)
def check_ip_port_valid(value):
    if not isinstance(value, str) or not value.strip():
        return False

    res = split_ip_port(value)
    if len(res) != 2:
        return False

    ip, port = res
    if check_ip_valid(ip) and check_port_valid(port):
        return True

    return False


@lru_cache(maxsize=None)
def check_port_valid(value):
    if isinstance(value, str):
        return str.isdigit(value) and 1023 < int(value) <= 65535
    elif isinstance(value, int):
        return 1023 < value <= 65535
    else:
        return False


@lru_cache(maxsize=None)
def check_instance_valid(value):
    if INSTANCE_PATTERN.match(value):
        return True

    return LOCAL_INSTANCE_PATTERN.match(value)


def is_more_permissive(filepath, max_permissions=0o600):
    return (os.stat(filepath).st_mode & 0o777) > max_permissions


def check_ssl_file_permission(certfile, keyfile, ca_file):
    if keyfile and is_more_permissive(keyfile, 0o400):
        result_msg = "WARNING: the permission of ssl key file %s is greater than 400." % keyfile
        write_to_terminal(result_msg, color="yellow")

    if certfile and is_more_permissive(certfile, 0o400):
        result_msg = "WARNING: the permission of ssl certificate file %s is greater than 400." % certfile
        write_to_terminal(result_msg, color="yellow")

    if ca_file and is_more_permissive(ca_file, 0o400):
        result_msg = "WARNING: the permission of ssl ca file %s is greater than 400." % ca_file
        write_to_terminal(result_msg, color="yellow")


@ignore_exc
def check_ssl_certificate_remaining_days(certfile, expired_threshold=90):
    """
    Check whether the certificate is expired or invalid.
    :param expired_threshold: how many days to warn.
    :param certfile: path of certificate.
    :certificate_warn_threshold: the warning days for certificate_remaining_days
    output: dict, check result which include 'check status' and 'check information'.
    """
    if not certfile:
        return
    gmt_format = '%b %d %H:%M:%S %Y GMT'
    child = subprocess.Popen(['openssl', 'x509', '-in', certfile, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[0]:
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = datetime.datetime.strptime(not_after, gmt_format)
        certificate_remaining_days = (end_time - datetime.datetime.now()).days
        if 0 < certificate_remaining_days < expired_threshold:
            result_msg = "WARNING: the certificate '{certificate}' has the remaining " \
                         "{certificate_remaining_days} days before out of date." \
                .format(certificate=certfile,
                        certificate_remaining_days=certificate_remaining_days)
            write_to_terminal(result_msg, color="yellow")
        elif certificate_remaining_days <= 0:
            result_msg = "WARNING: the certificate '{certificate}' is out of date." \
                .format(certificate=certfile)
            write_to_terminal(result_msg, color="yellow")


def warn_ssl_certificate(certfile, keyfile, ca_file):
    check_ssl_file_permission(certfile, keyfile, ca_file)
    check_ssl_certificate_remaining_days(certfile)


# The following utils are checking for command line arguments.
class CheckDSN(argparse.Action):
    @staticmethod
    def is_identifier_correct(url: str):
        # A correct DSN url is similar to:
        # 'postgres://{username}:{password}@{instance1},{instance2},{instance3}/{database}'
        # Hence, we can limit the number of identifiers to prevent bad url.
        identifiers = {
            '@': 1,
            '/': 3
        }
        for ident, limit in identifiers.items():
            if url.count(ident) > limit:
                return False, f"Incorrect URL because you haven't encoded the identifier '{ident}'."

        return True, None

    def __call__(self, parser, args, values, option_string=None):
        if values.startswith('postgres'):
            correct, msg = self.is_identifier_correct(values)
            if not correct:
                parser.error(msg)

        import psycopg2.extensions

        try:
            psycopg2.extensions.parse_dsn(values)
        except psycopg2.ProgrammingError:
            parser.error('This URL is an invalid dsn.')

        setattr(args, self.dest, values)


class CheckPort(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_port_valid(values):
            parser.error('Illegal port value(1024~65535): %s.' % values)

        setattr(args, self.dest, values)


class CheckIP(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_ip_valid(values):
            parser.error('Illegal IP: %s.' % values)

        setattr(args, self.dest, values)


class CheckAddress(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        res = split_ip_port(values)
        if len(res) != 2:
            if not check_ip_valid(values):
                parser.error('Illegal IP: %s.' % values)
        else:
            ip, port = res
            if not check_ip_valid(ip):
                parser.error('Illegal IP: %s.' % ip)
            if not check_port_valid(port):
                parser.error('Illegal port value(1024~65535): %s.' % port)

        setattr(args, self.dest, values)


class CheckWordValid(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                         "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
        if not values.strip():
            return

        if any(ill_char in values for ill_char in ill_character):
            parser.error('There are illegal characters in your input.')

        setattr(namespace, self.dest, values)


def existing_special_char(word):
    ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                     "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
    if word is not None and any(ill_char in word for ill_char in ill_character):
        return True

    return False


def path_type(path):
    realpath = os.path.realpath(path)
    if os.path.exists(realpath):
        return realpath

    raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


def http_scheme_type(param):
    param = param.lower()
    if param in ('http', 'https'):
        return param

    raise argparse.ArgumentTypeError('%s is not valid.' % param)


def positive_int_type(integer: str):
    if not integer.isdigit():
        raise argparse.ArgumentTypeError('Invalid value %s.' % integer)

    try:
        integer = int(integer)
    except ValueError:
        raise argparse.ArgumentTypeError('Invalid value %s.' % integer)

    if integer == 0:
        raise argparse.ArgumentTypeError('Invalid value 0.')

    return integer


def not_negative_int_type(integer: str):
    if not integer.isdigit():
        raise argparse.ArgumentTypeError('Invalid value %s.' % integer)

    try:
        integer = int(integer)
    except ValueError:
        raise argparse.ArgumentTypeError('Invalid value %s.' % integer)

    return integer


def date_type(date_str):
    """Cast date or timestamp string to
    a 13-bit timestamp integer."""
    if date_str.isdigit():
        # We can't know whether the timestamp users give is valid.
        # So, we only cast the string to integer and return it.
        return int(date_str)

    # If the date string users give is not a timestamp string, we
    # will regard it as the date time format.
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return int(d.timestamp() * 1000)  # unit: ms
    except ValueError:
        pass

    raise argparse.ArgumentTypeError('Invalid value %s.' % date_str)


def check_datetime_legality(time_string):
    try:
        datetime.datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def check_timestamp_legality(timestamp):
    # make sure the time unit is 'ms' which has 13 digits
    if not timestamp:
        return False

    return TIMESTAMPS_PATTERN.match(timestamp)


def check_name_valid(value):
    # avoid security issues such as injection based on whitelist
    if not value:
        return False

    return NAME_PATTERN.match(value)


def check_string_valid(value):
    # currently used to limit string length
    if isinstance(value, str):
        if not value:
            return False

        return len(value) <= MAX_STRING_LENGTH

    return False


def check_timezone_valid(value):
    # currently used to check format of timezone
    # note: now only support 'UTC'
    if not value:
        return False

    return TIMEZONE_PATTERN.match(value)


class ParameterChecker:
    UINT2 = 'uint2 (0 ~ 65535)'
    INT2 = 'int2 (-32768 ~ 32767)'
    TIMESTAMP = 'timestamp (ms)'
    PINT32 = 'Postive Interger (1 ~ 4294967295)'
    INT32 = 'Interger (0 ~ 4294967295)'
    PERCENTAGE = '0 ~ 1'
    BOOL = 'Bool'
    IP_WITHOUT_PORT = 'IP Without Port'
    IP_WITH_PORT = 'IP With Port'
    INSTANCE = 'IP With or Without Port'
    NAME = '2-120 Letters, Digits and Underlines'
    DIGIT = 'Digits String'
    STRING = 'String type, 1-10240 letters'
    TIMEZONE = 'Timezone type, UTC format, example: UTC-8, UTC+8:35'
    FLOAT = 'float type'

    @staticmethod
    def define_rules(**rules):
        """Used to determine whether the input parameters are legal."""

        def can_pass_inspection(parameter_pairs):
            for parameter, value in parameter_pairs.items():
                if parameter not in rules:
                    continue

                if value is None:
                    if rules[parameter]["optional"]:
                        continue
                    else:
                        return False, parameter
                try:
                    if rules[parameter]["type"] == ParameterChecker.UINT2:
                        if not (isinstance(value, int) and 0 <= value <= 65535):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.INT2:
                        if not (isinstance(value, int) and -32768 <= value <= 32767):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.PINT32:
                        if not (isinstance(value, int) and 0 < value <= 4294967295):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.INT32:
                        if not (isinstance(value, int) and 0 <= value <= 4294967295):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.PERCENTAGE:
                        if not (isinstance(value, (int, float)) and 0 <= value <= 1):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.BOOL:
                        if not isinstance(value, bool):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.IP_WITHOUT_PORT:
                        if not check_ip_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.IP_WITH_PORT:
                        if not check_ip_port_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.INSTANCE:
                        if not check_instance_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.NAME:
                        if not check_name_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.DIGIT:
                        if not (isinstance(value, str) and value.isdigit()):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.TIMESTAMP:
                        if not check_timestamp_legality(str(value)):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.STRING:
                        if not check_string_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.TIMEZONE:
                        if not check_timezone_valid(value):
                            return False, parameter
                    elif rules[parameter]["type"] == ParameterChecker.FLOAT:
                        if not isinstance(value, (int, float)):
                            return False, parameter

                except Exception:
                    return False, parameter

            return True, None

        def value_filter(parameter_pairs):
            for parameter, value in parameter_pairs.items():
                if parameter not in rules:
                    continue

            return parameter_pairs

        def decorator(f):

            @wraps(f)
            def wrapper(*args, **kwargs):
                success, parameter = can_pass_inspection(kwargs)
                if not success:
                    try:
                        raise ValueError(f"Incorrect value for parameter '{parameter}'.")
                    except ValueError as e:
                        logging.getLogger('uvicorn.error').exception(e)
                    return {'success': False, 'msg': 'Internal server error'}

                return f(*args, **value_filter(kwargs))

            return wrapper

        return decorator
